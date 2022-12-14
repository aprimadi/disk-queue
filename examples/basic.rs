use disk_queue::DiskQueue;

fn main() {
    let queue = DiskQueue::open("test.db");
    queue.enqueue("https://sahamee.com".as_bytes().to_vec());
    let item = queue.dequeue().unwrap();
    let s = std::str::from_utf8(&item).unwrap();
    println!("{}", s);
}

