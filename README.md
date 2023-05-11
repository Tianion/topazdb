# topazdb
A persistent key-value storage in rust, based on LSM tree.

**This project is still under development.**

example
```
fn main() {
    let storage = topazdb::opt::LsmOptions::default()
        .path("./")
        .open()
        .unwrap();
    storage.put(b"key", b"value").unwrap();
    let value = storage.get(b"key").unwrap();
    println!("value: {value:?}");
    storage.delete(b"key").unwrap();
    let value = storage.get(b"key").unwrap();
    println!("value: {value:?}");
}
```

## Reference
- https://github.com/tikv/agatedb
- https://github.com/skyzh/mini-lsm
- https://github.com/facebook/rocksdb
