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

## Benchmarks

### Setup

Benchmark is done with the following machine:

- Processor: i3-6100 @ 3.7GHz (total cores: 2, total threads: 4, 64KB L1, 512KB L2, 3MB L3 Cache)
- RAM: 24GB DDR4 @ 2133MHz
- Disk: Samsung SSD 850 Evo

We use fixed size record of length 6 to perform write / read (enqueue / dequeue).

### Write Throughput

Measured write throughput is about 9.7M writes/sec. This is about 58 MB/sec.

### Read Throughput

Measured read throughput is about 10.0 writes/sec. This is about 60 MB/sec.

## Limitations

The size of the item stored in the queue cannot be greater than 4089 bytes. 
This is due to the page size is set to 4096 bytes (4KB). If you need to store
items larger than 4089 bytes, consider forking this repository and change the 
page size. 

That being said, performance will degrade as the item size gets 
larger (> 512 bytes) because it needs to perform file copy frequently.

