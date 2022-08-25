# Disk Queue

FIFO queue backed by disk.

## Usage

```rust
use disk_queue::DiskQueue;

let mut queue = DiskQueue::new("test.db");
queue.enqueue("https://sahamee.com".as_bytes().to_vec());
let item = queue.dequeue().unwrap();
let s = std::str::from_utf8(&item).unwrap();
println!("{}", s); // print "https://sahamee.com"
```

## Limitations

The size of the item stored in the queue cannot be greater than 4089 bytes. 
This is due to the page size is set to 4096 bytes (4KB). If you need to store
items larger than 4089 bytes, consider forking this repository and change the 
page size. 

That being said, performance will degrade as the item size gets 
larger (> 512 bytes) because it needs to perform file copy frequently.

