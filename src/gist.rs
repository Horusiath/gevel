use crate::{gist_page_is_leaf, item_ptr_get_blk_num, Buffer, Page, GIST_ROOT_BLKNO, PAGE_SIZE};
use pgx::pg_sys::{
    index_close, index_open, AccessExclusiveLock, BlockNumber, GISTPageOpaqueData, IndexTupleData,
    InvalidBlockNumber, OffsetNumber, Oid, Relation,
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

    pub fn get_tree(&self, max_level: Option<usize>) -> IndexTree {
        let node = self.get_tree_node(0, max_level, GIST_ROOT_BLKNO, 0);
        IndexTree(node)
    }

    fn get_tree_node(
        &self,
        level: usize,
        max_level: Option<usize>,
        blk: BlockNumber,
        offset: OffsetNumber,
    ) -> IndexTreeNode {
        let buf = Buffer::new(self.relation, blk);
        let page = Page::new(buf);
        let max_offset = page.max_offset();
        let free_space = page.free_space();
        let opaque: &GISTPageOpaqueData = page.as_special();
        let is_leaf = gist_page_is_leaf(opaque);

        let mut node = IndexTreeNode::new(
            max_offset,
            free_space,
            offset,
            blk,
            opaque.rightlink,
            is_leaf,
        );

        if !is_leaf {
            let recurse = match max_level {
                Some(max) => max > level,
                None => true,
            };

            if recurse {
                let children = node.children.as_mut().unwrap();
                for i in 1..=max_offset {
                    let iid = page.item_id(i as usize);
                    let which: &IndexTupleData =
                        unsafe { (page.get_item(iid) as *mut IndexTupleData).as_ref() }
                            .expect("PageGetItem failed");
                    let cblk = item_ptr_get_blk_num(which.t_tid);
                    let child = self.get_tree_node(level + 1, max_level, cblk, i);
                    children.push(child);
                }
            }
        }

        node
    }
}

impl Drop for IndexInspector {
    fn drop(&mut self) {
        unsafe { index_close(self.relation, AccessExclusiveLock as i32) }
    }
}

pub struct IndexTree(IndexTreeNode);

struct IndexTreeNode {
    offset: OffsetNumber,
    max_offset: OffsetNumber,
    block_num: BlockNumber,
    free_space: usize,
    right_link: Option<BlockNumber>,
    children: Option<Vec<IndexTreeNode>>,
}

impl IndexTreeNode {
    fn new(
        max_offset: OffsetNumber,
        free_space: usize,
        offset: OffsetNumber,
        block_num: BlockNumber,
        right: BlockNumber,
        is_leaf: bool,
    ) -> Self {
        IndexTreeNode {
            max_offset,
            free_space,
            offset,
            block_num,
            right_link: if right == InvalidBlockNumber {
                None
            } else {
                Some(right)
            },
            children: if is_leaf { None } else { Some(Vec::new()) },
        }
    }

    fn is_leaf(&self) -> bool {
        self.children.is_none()
    }

    /// Returns a value from [0.0..1.0] which describes the percentage of space occupied by data
    /// inside of current page.
    fn occupied(&self) -> f64 {
        (PAGE_SIZE as f64 - self.free_space as f64) / PAGE_SIZE as f64
    }

    fn fmt(&self, f: &mut Formatter<'_>, level: usize) -> std::fmt::Result {
        writeln!(
            f,
            "{}{}(l:{}) blk: {} numTuple: {} free: {}B ({:.2}%) rightlink: {}",
            format!("{:width$}", "", width = level * 4),
            self.offset,
            level,
            self.block_num,
            self.max_offset,
            self.free_space,
            self.occupied() * 100.0,
            match self.right_link {
                None => "Invalid Block".to_string(),
                Some(blk) => blk.to_string(),
            }
        )?;

        if let Some(children) = self.children.as_ref() {
            for node in children.iter() {
                node.fmt(f, level + 1)?;
            }
        }

        Ok(())
    }
}

impl Display for IndexTree {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f, 0)
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
