use crate::{Buffer, Page, GIST_ROOT_BLKNO, PAGE_SIZE};
use pgx::pg_sys::{
    index_close, index_open, AccessExclusiveLock, BlockNumber, FirstOffsetNumber,
    GISTPageOpaqueData, InvalidBlockNumber, OffsetNumber, Oid, Relation, BLCKSZ, F_LEAF,
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
        let gist_page = GistPage::new(&page);
        let is_leaf = gist_page.is_leaf();

        let mut node = IndexTreeNode::new(
            max_offset,
            free_space,
            offset,
            blk,
            gist_page.right_link(),
            is_leaf,
        );

        if !is_leaf {
            let recurse = match max_level {
                Some(max) => max > level,
                None => true,
            };

            if recurse {
                let children = node.children.as_mut().unwrap();
                for i in FirstOffsetNumber..=max_offset {
                    let iid = page.item_id(i as usize);
                    let which = page.get_index_tuple(iid);
                    let cblk = which.block_num();
                    let child = self.get_tree_node(level + 1, max_level, cblk, i);
                    children.push(child);
                }
            }
        }

        node
    }

    pub fn stats(&self, max_level: Option<usize>) -> Stats {
        let mut stats = Stats::default();
        self.stats_inner(0, max_level, GIST_ROOT_BLKNO, &mut stats);
        stats
    }

    fn stats_inner(
        &self,
        level: usize,
        max_level: Option<usize>,
        blk: BlockNumber,
        stats: &mut Stats,
    ) {
        let buf = Buffer::new(self.relation, blk);
        let page = Page::new(buf);
        let max_offset = page.max_offset();
        let tuple_size = PAGE_SIZE as u64 - page.free_space() as u64;
        let gist_page = GistPage::new(&page);
        let is_leaf = gist_page.is_leaf();

        stats.num_pages += 1;
        stats.tuple_size += tuple_size;
        stats.total_size += BLCKSZ as u64;
        stats.num_tuple += max_offset as usize;
        stats.level = stats.level.max(level);

        if is_leaf {
            stats.num_leaf_pages += 1;
            stats.leaf_tuple_size += tuple_size;
            stats.num_leaf_tuple += max_offset as usize;
        } else {
            for i in FirstOffsetNumber..=max_offset {
                let iid = page.item_id(i as usize);
                let which = page.get_index_tuple(iid);
                if which.is_invalid() {
                    stats.num_invalid_tuple += 1;
                }
                let cblk = which.block_num();
                self.stats_inner(level + 1, max_level, cblk, stats);
            }
        }
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

pub struct Stats {
    /// Max level of depth of index tree.
    pub level: usize,
    /// How many pages consist on the current index.
    pub num_pages: usize,
    /// How many leaf pages consist on the current index.
    pub num_leaf_pages: usize,
    /// How many leaf tuples consist on the current index.
    pub num_leaf_tuple: usize,
    /// How many tuples does current index has.
    pub num_tuple: usize,
    /// How many tuples are invalid.
    pub num_invalid_tuple: usize,
    /// Total size of memory occupied by tuples in bytes.
    pub tuple_size: u64,
    /// Size of memory occupied by leaf tuples in bytes.
    pub leaf_tuple_size: u64,
    /// Total size of an index (includes both total tuple_size
    /// and total free page space reserved for future use).  
    pub total_size: u64,
}

impl Default for Stats {
    fn default() -> Self {
        Stats {
            level: 0,
            num_pages: 0,
            num_leaf_pages: 0,
            num_tuple: 0,
            num_invalid_tuple: 0,
            num_leaf_tuple: 0,
            tuple_size: 0,
            leaf_tuple_size: 0,
            total_size: 0,
        }
    }
}

impl Display for Stats {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Number of levels:          {}", self.level + 1)?;
        writeln!(f, "Number of pages:           {}", self.num_pages)?;
        writeln!(f, "Number of leaf pages:      {}", self.num_leaf_pages)?;
        writeln!(f, "Number of tuples:          {}", self.num_tuple)?;
        writeln!(f, "Number of invalid tuples:  {}", self.num_invalid_tuple)?;
        writeln!(f, "Number of leaf tuples:     {}", self.num_leaf_tuple)?;
        writeln!(f, "Total size of tuples:      {} bytes", self.tuple_size)?;
        writeln!(
            f,
            "Total size of leaf tuples: {} bytes",
            self.leaf_tuple_size
        )?;
        writeln!(f, "Total size of index:       {} bytes", self.total_size)
    }
}

struct GistPage<'a> {
    opaque: &'a GISTPageOpaqueData,
}

impl<'a> GistPage<'a> {
    fn new(page: &'a Page) -> Self {
        let opaque = page.as_special();
        GistPage { opaque }
    }

    fn is_leaf(&self) -> bool {
        self.opaque.flags as u32 == F_LEAF
    }

    fn right_link(&self) -> BlockNumber {
        self.opaque.rightlink
    }
}
