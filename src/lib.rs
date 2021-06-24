#![feature(const_maybe_uninit_as_ptr)]
#![feature(const_raw_ptr_deref)]
#![feature(const_raw_ptr_to_usize_cast)]

mod gist;

use crate::gist::IndexInspector;
use memoffset::offset_of;
use pgx::pg_sys::{
    BlockNumber, BufferGetPage, Item, ItemIdData, ItemPointerData, Oid, PageGetFreeSpace,
    PageHeaderData, RangeVar, RangeVarGetRelidExtended, ReadBuffer, Relation, ReleaseBuffer,
    BLCKSZ, MAXIMUM_ALIGNOF,
};
use pgx::*;
use std::mem::size_of;
use std::ptr::null_mut;

pg_module_magic!();

#[pg_extern]
pub fn gist_tree(rel_oid: Oid) -> String {
    let index = IndexInspector::open(rel_oid);
    let tree = index.get_tree(None);
    tree.to_string()
}

/// Wrapper around PostgreSQL page buffer.
struct Buffer(pg_sys::Buffer);

impl Buffer {
    fn new(rel: Relation, blk: BlockNumber) -> Self {
        Buffer(unsafe { ReadBuffer(rel, blk) })
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        unsafe { ReleaseBuffer(self.0) }
    }
}

pub static PAGE_SIZE: u32 = BLCKSZ
    - ((((size_of::<PageHeaderData>() + size_of::<ItemIdData>()) as u32) + (MAXIMUM_ALIGNOF - 1))
        & !(MAXIMUM_ALIGNOF - 1));

pub const GIST_ROOT_BLKNO: BlockNumber = 0;

/// Wrapper around PostgreSQL Page, equipped with convenient safe API for common operations.
struct Page(pg_sys::Page, Buffer); // keep the buffer around, so it's not prematurely released

impl Page {
    pub fn new(buf: Buffer) -> Self {
        let page_ptr = unsafe { BufferGetPage(buf.0) };
        Page(page_ptr, buf)
    }

    fn header(&self) -> &PageHeaderData {
        unsafe { (self.0 as *mut PageHeaderData).as_ref() }.expect("PageHeader was NULL")
    }

    pub fn max_offset(&self) -> u16 {
        let header = self.header();
        let size_of_page_header_data = offset_of!(PageHeaderData, pd_linp) as u16;

        if header.pd_lower <= size_of_page_header_data {
            0
        } else {
            (header.pd_lower - size_of_page_header_data) / size_of::<ItemIdData>() as u16
        }
    }

    pub fn is_special(&self) -> bool {
        match unsafe { (self.0 as *mut PageHeaderData).as_ref() } {
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

    pub fn as_special<T>(&self) -> &T {
        assert!(self.is_special());
        unsafe {
            let ptr = self.0 as *mut u8;
            let header = self.header();
            let special = ptr.offset(header.pd_special as isize) as *mut T;
            special.as_ref().expect("Couldn't map page to special")
        }
    }

    pub fn item_id(&self, offset: usize) -> ItemIdData {
        let pd_linp = unsafe { self.header().pd_linp.as_slice(offset) };
        pd_linp[offset - 1]
    }

    pub fn get_item(&self, item_id: ItemIdData) -> Item {
        unsafe { (self.0 as *mut u8).offset(item_id.lp_off() as isize) as Item }
    }

    pub fn free_space(&self) -> usize {
        unsafe { PageGetFreeSpace(self.0) }
    }
}

fn item_ptr_get_blk_num(ptr: ItemPointerData) -> BlockNumber {
    let block_id = ptr.ip_blkid;
    (((block_id.bi_hi as u32) << 16) | (block_id.bi_lo as u32)) as BlockNumber
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
