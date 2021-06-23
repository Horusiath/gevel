#![feature(const_maybe_uninit_as_ptr)]
#![feature(const_raw_ptr_deref)]
#![feature(const_raw_ptr_to_usize_cast)]

use memoffset::offset_of;
use pgx::pg_sys::{
    index_close, index_open, AccessExclusiveLock, BlockNumber, BufferGetPage, FirstOffsetNumber,
    GISTPageOpaque, GISTPageOpaqueData, IndexTuple, IndexTupleData, InvalidBlockNumber, Item,
    ItemId, ItemIdData, ItemPointerData, LocationIndex, OffsetNumber, Oid, Page, PageGetFreeSpace,
    PageHeader, PageHeaderData, RangeVar, RangeVarGetRelidExtended, ReadBuffer, Relation,
    ReleaseBuffer, BLCKSZ, F_LEAF, MAXIMUM_ALIGNOF,
};
use pgx::*;
use std::fmt::{Display, Formatter};
use std::mem::size_of;
use std::ptr::null_mut;

pg_module_magic!();

#[pg_extern]
pub fn gist_tree(rel_oid: Oid) -> String {
    let index = IndexDescriptor::open(rel_oid);
    index.to_string()
}

static PAGE_SIZE: u32 = BLCKSZ
    - ((((size_of::<PageHeaderData>() + size_of::<ItemIdData>()) as u32) + (MAXIMUM_ALIGNOF - 1))
        & !(MAXIMUM_ALIGNOF - 1));
const GIST_ROOT_BLKNO: BlockNumber = 0;

fn page_max_offset_number(page: Page) -> u16 {
    let header: &PageHeaderData =
        unsafe { (page as *mut PageHeaderData).as_ref() }.expect("PageHeader was NULL");
    let size_of_page_header_data = offset_of!(PageHeaderData, pd_linp) as u16;

    if header.pd_lower <= size_of_page_header_data {
        0
    } else {
        (header.pd_lower - size_of_page_header_data) / size_of::<ItemIdData>() as u16
    }
}

unsafe fn page_is_special_ptr(page: Page) -> bool {
    match (page as *mut PageHeaderData).as_ref() {
        None => false,
        Some(p) => {
            let i = p.pd_special;
            if i <= BLCKSZ as u16 && i >= offset_of!(PageHeaderData, pd_linp) as u16 {
                true
            } else {
                false
            }
        }
    }
}

unsafe fn page_get_special_ptr<T>(page: Page) -> *mut T {
    assert!(page_is_special_ptr(page));
    let ptr = page as *mut u8;
    let header = (page as *mut PageHeaderData)
        .as_ref()
        .expect("PageHeaderData is NULL");
    ptr.offset(header.pd_special as isize) as *mut T
}

unsafe fn gist_page_get_opaque(page: Page) -> *mut GISTPageOpaqueData {
    page_get_special_ptr(page)
}

#[inline]
fn gist_page_is_leaf(page: &GISTPageOpaqueData) -> bool {
    page.flags as u32 == F_LEAF
}

fn page_get_item_id(page: Page, offset: usize) -> ItemIdData {
    //  ((ItemId) (&((PageHeader) (page))->pd_linp[(offsetNumber) - 1]))
    let p: &PageHeaderData =
        unsafe { (page as *mut PageHeaderData).as_ref() }.expect("PageHeaderData was NULL");
    let pd_linp = unsafe { p.pd_linp.as_slice(offset) };
    pd_linp[offset - 1]
}

fn page_get_item(page: Page, item_id: ItemIdData) -> Item {
    unsafe { (page as *mut u8).offset(item_id.lp_off() as isize) as Item }
}

fn item_ptr_get_blk_num(ptr: ItemPointerData) -> BlockNumber {
    let block_id = ptr.ip_blkid;
    (((block_id.bi_hi as u32) << 16) | (block_id.bi_lo as u32)) as BlockNumber
}

struct Buffer(pg_sys::Buffer);

impl Buffer {
    fn new(rel: Relation, blk: BlockNumber) -> Self {
        Buffer(unsafe { ReadBuffer(rel, blk) })
    }

    fn page(&self) -> pg_sys::Page {
        unsafe { BufferGetPage(self.0) }
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        unsafe { ReleaseBuffer(self.0) }
    }
}

struct IndexDescriptor {
    relation: Relation,
}

impl IndexDescriptor {
    fn open(rel_oid: Oid) -> Self {
        let relation = unsafe { index_open(rel_oid, AccessExclusiveLock as i32) };
        IndexDescriptor { relation }
    }

    unsafe fn dump_tree(
        &self,
        f: &mut Formatter<'_>,
        level: usize,
        max_level: Option<usize>,
        blk: BlockNumber,
        coff: OffsetNumber,
    ) -> std::fmt::Result {
        let buffer = Buffer::new(self.relation, blk);
        let page = buffer.page();
        let max_offset = page_max_offset_number(page);
        let free_space = PageGetFreeSpace(page);
        let free_space_coef = (100.0 * (PAGE_SIZE as f64 - free_space as f64)) / PAGE_SIZE as f64;
        let opaque: &GISTPageOpaqueData = gist_page_get_opaque(page)
            .as_ref()
            .expect("GistGetPageOpaque failed");

        writeln!(
            f,
            "{}{}(l:{}) blk: {} numTuple: {} free: {}B ({:.2}%) rightlink: {} ({})",
            format!("{:width$}", "", width = level * 4),
            coff,
            level,
            blk,
            max_offset,
            free_space,
            free_space_coef,
            opaque.rightlink as u32,
            if opaque.rightlink == InvalidBlockNumber {
                "InvalidBlockNumber"
            } else {
                "OK"
            }
        )?;

        if !gist_page_is_leaf(opaque) && (max_level.is_none() || level < max_level.unwrap()) {
            for i in 1..=max_offset {
                let iid = page_get_item_id(page, i as usize);
                let which: &IndexTupleData = (page_get_item(page, iid) as *mut IndexTupleData)
                    .as_ref()
                    .expect("PageGetItem failed");
                let cblk = item_ptr_get_blk_num(which.t_tid);
                self.dump_tree(f, level + 1, max_level, cblk, i)?;
            }
        }

        Ok(())
    }
}

impl Drop for IndexDescriptor {
    fn drop(&mut self) {
        unsafe { index_close(self.relation, AccessExclusiveLock as i32) }
    }
}

impl Display for IndexDescriptor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        unsafe { self.dump_tree(f, 0, None, GIST_ROOT_BLKNO, 0) }
    }
}

struct IndexStat {
    level: i32,
    num_pages: i32,
    num_leaf_pages: i32,
    num_tuple: i32,
    num_invalid_tuple: i32,
    num_leaf_tuple: i32,
    tuples_size: u64,
    leaf_tuple_size: u64,
    total_size: u64,
}

struct IndexInfo {
    max_level: Option<u32>,
    txt: String,
    len: i32,
}

#[inline(always)]
fn range_var_get_rel_id(rel_var: &RangeVar, lock_mode: u32) -> Oid {
    unsafe { RangeVarGetRelidExtended(rel_var, lock_mode as i32, 0, None, null_mut()) }
}

//#[pg_extern]
//pub fn gist_print() {
//    todo!()
//}
//
//#[pg_extern]
//pub fn gist_stat() {
//    todo!()
//}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use pgx::*;

    //#[pg_test]
    //fn test_hello_gevel() {
    //    assert_eq!("Hello, gevel", crate::hello_gevel());
    //}
}

#[cfg(test)]
pub mod pg_test {
    //pub fn setup(_options: Vec<&str>) {
    //    // perform one-off initialization when the pg_test framework starts
    //}
    //
    //pub fn postgresql_conf_options() -> Vec<&'static str> {
    //    // return any postgresql.conf settings that are required for your tests
    //    vec![]
    //}
}
