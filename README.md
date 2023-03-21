# topazdb
A persistent key-value storage in rust, based on LSM tree.

**This project is still under development.**

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
  - [x] Block Compress 
  - [x] Block Cache
  - [ ] Write Batch
  - [ ] Align blocks to 4K 
  - [ ] Bloom Filter
  - [x] Graceful Shutdown and Cleanup
  - [x] Compact Options
- [ ] More test
  - [ ] Bench Test
  - [ ] Shutdown Test
- [ ] Undetermined
  - [ ] Direct I/O
  - [ ] Async iterator
  - [ ] Secondary key support 
  - [ ] Network

## Reference
- https://github.com/tikv/agatedb
- https://github.com/skyzh/mini-lsm
- https://github.com/facebook/rocksdb