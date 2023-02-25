# topazdb
A persistent key-value storage in rust, based on LSM tree.

This project is still under development.

## Plan
- [x] Basic features
  - [x] Block
  - [x] Sst
  - [x] Memtable
  - [x] Iterators
  - [x] Lsm storage
  - [x] Leveled compaction
  - [x] WAL and recovery
  - [X] Basic Test
- [ ] Optimize
  - [ ] Checksum
    - [ ] Block
    - [ ] Sst 
  - [ ] Block Compress 
  - [x] Block Cache
  - [ ] Write Batch
  - [ ] Align blocks to 4K 
  - [ ] Bloom Filter
  - [ ] Graceful Shutdown and Cleanup
  - [ ] Compact Options
- [ ] More test
  - [ ] Bench Test
  - [ ] Shutdown Test

## not sure whether to do
- Direct I/O
- Async iterator
- Secondary key support 

## Reference
- https://github.com/tikv/agatedb
- https://github.com/skyzh/mini-lsm
- https://github.com/facebook/rocksdb