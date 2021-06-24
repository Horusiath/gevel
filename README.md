# Gevel

This Postgres extension - implemented in Rust - is derived from original [C gevel extension](http://www.sai.msu.su/~megera/wiki/Gevel), mostly for learning purposes.

This project uses [rust pgx](https://github.com/zombodb/pgx) library. Given all necessary dependencies are in place, you can run it via:

```bash
# Start PostgreSQL v13 database
cargo pgx run pg13
```

Then from within pgsql console:

```sql
CREATE EXTENSION gevel;

-- sample data
CREATE TABLE books(id serial primary key, title text, q tsvector);
INSERT INTO books(title) VALUES('The Colour of Magic');
INSERT INTO books(title) VALUES('Equal Rites');
INSERT INTO books(title) VALUES('Night Watch');
UPDATE books SET q = to_tsvector('English', title);
CREATE INDEX gist_book_title on books using gist(q);

-- atm gevel uses oids, so we need to get oid for our index
SELECT oid FROM pg_class WHERE relname = 'gist_book_title';
-- finally display the contents of our gist index
SELECT gist_tree(_oid_returned_from_previous_query_);
```

Returned data may look like this:

```
                                 gist_tree                                   
------------------------------------------------------------------------------
 0(l:0) blk: 0 numTuple: 6 free: 7260B (11.03%) rightlink: Invalid Block     
     1(l:1) blk: 1 numTuple: 38 free: 2548B (68.77%) rightlink: 2            
     2(l:1) blk: 3 numTuple: 31 free: 2624B (67.84%) rightlink: Invalid Block
     3(l:1) blk: 4 numTuple: 23 free: 3928B (51.86%) rightlink: 3            
     4(l:1) blk: 2 numTuple: 33 free: 2000B (75.49%) rightlink: 5            
     5(l:1) blk: 5 numTuple: 24 free: 4420B (45.83%) rightlink: 6            
     6(l:1) blk: 6 numTuple: 32 free: 2348B (71.23%) rightlink: 4            
```

Printed gist tree applied left-pad nesting for child pages. Numbers are as follows:

- `1` Page block offset.
- `(l:1)` - level of nesting in index B+tree structure
- `blk: 1` - block number.
- `numTuple: 38` - number of tuples stored on that page
- `free: 2548B` - number of free space left on that page (Postgres pages by default are 8KiB).
- `(68.77%)` - how much of the page space is occupied.
- `rightlink: 2` - block number of the next page if any.

Another function is `gist_stat(oid)` which returns an aggregated statistics about the index:

```
               gist_stat                
----------------------------------------
 Number of levels:          2          
 Number of pages:           7          
 Number of leaf pages:      6          
 Number of tuples:          187        
 Number of invalid tuples:  0          
 Number of leaf tuples:     181        
 Total size of tuples:      31992 bytes
 Total size of leaf tuples: 31092 bytes
 Total size of index:       57344 bytes
```