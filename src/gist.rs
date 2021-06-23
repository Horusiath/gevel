use crate::{gist_page_is_leaf, item_ptr_get_blk_num, Buffer, GIST_ROOT_BLKNO, PAGE_SIZE};
use pgx::pg_sys::{
    index_close, index_open, AccessExclusiveLock, BlockNumber, GISTPageOpaqueData, IndexTupleData,
    InvalidBlockNumber, OffsetNumber, Oid, PageGetFreeSpace, Relation,
};
use std::fmt::{Display, Formatter};

pub struct IndexInspector {
    relation: Relation,
}

impl IndexInspector {
    pub fn open(rel_oid: Oid) -> Self {
        let relation = unsafe { index_open(rel_oid, AccessExclusiveLock as i32) };
        IndexInspector { relation }
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
        let max_offset = page.max_offset();
        let free_space = page.free_space();
        let free_space_coef = (100.0 * (PAGE_SIZE as f64 - free_space as f64)) / PAGE_SIZE as f64;
        let opaque: &GISTPageOpaqueData = page.as_special();

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
                let iid = page.item_id(i as usize);
                let which: &IndexTupleData = (page.get_item(iid) as *mut IndexTupleData)
                    .as_ref()
                    .expect("PageGetItem failed");
                let cblk = item_ptr_get_blk_num(which.t_tid);
                self.dump_tree(f, level + 1, max_level, cblk, i)?;
            }
        }

        Ok(())
    }
}

impl Drop for IndexInspector {
    fn drop(&mut self) {
        unsafe { index_close(self.relation, AccessExclusiveLock as i32) }
    }
}

impl Display for IndexInspector {
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
