use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

use memmap::{MmapMut, MmapOptions};

mod constant;
mod page;

use constant::PAGE_SIZE;
use page::{
    buf_write_metadata_page, empty_page_buf,
    Cursor, Metadata, MetaPage, RecordPage
};

pub struct DiskQueue {
    file: Arc<Mutex<File>>,
    meta_page: Arc<RwLock<MetaPage>>,
    read_page: Arc<RwLock<RecordPage>>,
    write_page: Arc<RwLock<RecordPage>>,
    // This protects reading/writing from write_page_mem
    rwlatch: Arc<RwLock<()>>,
    // Cast to usize so it can be sent safely between threads
    // Real type is `*const u8`
    write_page_mem: usize,
    _mmap: MmapMut,
}

impl DiskQueue {
    pub fn new(path: &str) -> Self {
        // Check if file exists, if it doesn't initialize file and close it
        if !Path::new(path).exists() {
            let mut file = File::create(path).unwrap();
            let meta = Metadata {
                num_pages: 1,
                num_items: 0,
                read_cursor: Cursor { pageid: 1, slotid: 0 },
                write_cursor: Cursor { pageid: 1, slotid: 0 },
            };
            let meta_page_buf = buf_write_metadata_page(&meta);
            let write_page_buf = empty_page_buf();
            assert_eq!(meta_page_buf.len(), PAGE_SIZE);
            assert_eq!(write_page_buf.len(), PAGE_SIZE);
            file.write(&meta_page_buf).unwrap();
            file.write(&write_page_buf).unwrap();
            file.sync_all().unwrap();
        }
        
        // Open the file, mmap-ing the first two pages
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let mmap = unsafe {
            MmapOptions::new()
                .len(2 * PAGE_SIZE)
                .map_mut(&file)
                .unwrap()
        };
        
        let meta_page_mem = mmap.as_ptr() as usize;
        let write_page_mem = unsafe {
            mmap.as_ptr().add(PAGE_SIZE)
        } as usize;
        let file = Arc::new(Mutex::new(file));

        let rwlatch = Arc::new(RwLock::new(()));
        
        let meta_page = Arc::new(RwLock::new(MetaPage::from_mmap_ptr(meta_page_mem)));
        let write_page = Arc::new(RwLock::new(
            RecordPage::from_mmap_ptr(rwlatch.clone(), write_page_mem))
        );
        
        let read_page;
        {
            let meta_page = meta_page.write().unwrap();
            let num_pages = meta_page.get_num_pages();
            let read_cursor = meta_page.get_read_cursor();
            if read_cursor.pageid == num_pages {
                read_page = Arc::new(RwLock::new(
                    RecordPage::from_mmap_ptr(rwlatch.clone(), write_page_mem)
                ));
            } else {
                read_page = Arc::new(RwLock::new(
                    RecordPage::from_file(file.clone(), read_cursor.pageid)
                ));
            }
        }

        Self {
            file,
            meta_page,
            read_page,
            write_page,
            write_page_mem,
            rwlatch,
            _mmap: mmap,
        }
    }
    
    pub fn num_items(&self) -> u64 {
        let meta_page = self.meta_page.read().unwrap();
        meta_page.get_num_items()
    }

    pub fn enqueue(&self, record: Vec<u8>) {
        let mut meta_page = self.meta_page.write().unwrap();
        let mut write_page = self.write_page.write().unwrap();
        if write_page.can_insert(&record) {
            // Case 1: the write page can still hold the record

            write_page.insert(record);
            
            meta_page.incr_num_items();
            let mut write_cursor = meta_page.get_write_cursor();
            write_cursor.slotid += 1;
            meta_page.set_write_cursor(write_cursor);
        } else {
            // Case 2: the write page cannot hold the new record
            //
            // This should write the page to disk and reset the write page
            
            // Copy write page to a new page and reset write page
            let pageid = meta_page.get_num_pages();
            write_page.save(self.file.clone(), pageid);
            write_page.reset();
            
            write_page.insert(record);

            let mut write_cursor = meta_page.get_write_cursor();
            let mut read_cursor = meta_page.get_read_cursor();
            
            // There are two cases here:
            // 1. Read page points to the write page (i.e. it shares the same 
            //    underlying memory)
            // 2. Read page points to a read-only page from disk.
            //
            // In case 2, we don't have to do anything.
            //
            // In case 1, we further need to determine if read_cursor is 
            // equal to write_cursor. 
            // If it is, then the read page should still point to write page. 
            // Nothing should be done.
            // If it is not, we need to load read_page from a recently written 
            // page.
            {
                let mut read_page = self.read_page.write().unwrap();
                if read_page.is_shared_mem() && 
                   read_cursor != write_cursor 
                {
                    let rp = RecordPage::from_file(
                        self.file.clone(),
                        pageid,
                    );
                    *read_page = rp;
                }
            }

            // Note that slotid is 1 since we just inserted a new record on 
            // the newly inserted page
            //
            // Also, we need to fix read cursor to point to a new page if it 
            // points to the same cursor as write cursor
            if read_cursor == write_cursor {
                read_cursor.pageid += 1;
                read_cursor.slotid = 0;
                meta_page.set_read_cursor(read_cursor);
            }
            write_cursor.pageid += 1;
            write_cursor.slotid = 1;

            meta_page.incr_num_items();
            meta_page.incr_num_pages();
            meta_page.set_write_cursor(write_cursor);
        }
    }

    pub fn dequeue(&self) -> Option<Vec<u8>> {
        let mut meta_page = self.meta_page.write().unwrap();
        
        let mut assign_write_to_read_page = false;
        let mut read_next_page = false;
        let mut read_cursor;
        let record;
        {
            let read_page = self.read_page.read().unwrap();
            
            let num_pages = meta_page.get_num_pages();
            let num_records = read_page.num_records();
            read_cursor = meta_page.get_read_cursor();
            let write_cursor = meta_page.get_write_cursor();
            
            if read_cursor == write_cursor {
                return None;
            }
            
            match read_page.get_record(read_cursor.slotid as usize) {
                Some(r) => record = r,
                None => panic!("Invariant violated"),
            }
            if read_cursor.slotid + 1 < num_records as u16 || 
               read_cursor.pageid == write_cursor.pageid {
                read_cursor.slotid += 1;
                meta_page.set_read_cursor(read_cursor.clone());
            } else {
                read_cursor.pageid += 1;
                read_cursor.slotid = 0;
                meta_page.set_read_cursor(read_cursor.clone());
                
                assert!(read_cursor.pageid <= num_pages);
                if read_cursor.pageid == num_pages {
                    assign_write_to_read_page = true;
                } else {
                    read_next_page = true;
                }
            }
        }
        
        if assign_write_to_read_page {
            let mut read_page = self.read_page.write().unwrap();
            *read_page = RecordPage::from_mmap_ptr(
                self.rwlatch.clone(), 
                self.write_page_mem,
            );
        } else if read_next_page {
            let mut read_page = self.read_page.write().unwrap();
            *read_page = RecordPage::from_file(
                self.file.clone(),
                read_cursor.pageid,
            );
        }
        
        Some(record)
    }
}

#[cfg(test)]
mod tests {
    use rand::RngCore;
    use std::sync::Condvar;
    use std::sync::atomic::{AtomicU32, Ordering};
    use super::*;

    const TEST_DB_PATH: &str = "test.db";

    fn cleanup_test_db() {
        loop {
            std::fs::remove_file(TEST_DB_PATH).unwrap();
            if !Path::new(TEST_DB_PATH).exists() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    #[test]
    fn basic() {
        {
            let records = vec![
                "https://www.google.com".as_bytes().to_vec(),
                "https://www.dexcode.com".as_bytes().to_vec(),
                "https://sahamee.com".as_bytes().to_vec(),
            ];
        
            let queue = DiskQueue::new(TEST_DB_PATH);
            for record in records.iter() {
                queue.enqueue(record.clone());
            }

            let mut popped_records = vec![];
            loop {
                match queue.dequeue() {
                    Some(record) => popped_records.push(record),
                    None => break,
                }
            }
            
            assert_eq!(records, popped_records);
        }

        cleanup_test_db();
    }

    fn test_read_write_single_threaded(
        num_records: usize, 
        read_ratio: u32, 
        write_ratio: u32
    ) {
        assert_eq!((read_ratio + write_ratio) & 1, 0);

        {
            let mut records = vec![];
            for i in 0..num_records {
                let s = format!("record_{}", i);
                records.push(s.as_bytes().to_vec());
            }

            let mut popped_records = vec![];

            let queue = DiskQueue::new(TEST_DB_PATH);

            let mut enqueue_finished = false;
            let mut dequeue_finished = false;
            let mut rng = rand::thread_rng();
            let mut records_iter = records.iter();
            loop {
                let num = rng.next_u32() % (read_ratio + write_ratio);
                if num < read_ratio {
                    // Dequeue
                    match queue.dequeue() {
                        Some(r) => {
                            popped_records.push(r)
                        }
                        None => {
                            if enqueue_finished {
                                dequeue_finished = true;
                            }
                        }
                    }
                } else {
                    // Enqueue
                    match records_iter.next() {
                        Some(r) => queue.enqueue(r.clone()),
                        None => enqueue_finished = true,
                    }
                }

                if enqueue_finished && dequeue_finished {
                    break;
                }
            }

            for (idx, record) in records.iter().enumerate() {
                let empty_vec = vec![];
                let popped_record = popped_records.get(idx).unwrap_or(&empty_vec);
                assert_eq!(
                    String::from_utf8_lossy(record), 
                    String::from_utf8_lossy(popped_record)
                );
            }
        }

        cleanup_test_db();
    }
    
    #[test]
    // Test reading & writing a lot of pages with read-write ratio of 1:3
    fn multiple_pages() {
        test_read_write_single_threaded(10000, 1, 3);
    }
    
    #[test]
    // Test reading & writing a lot of pages with read-write ratio of 3:1
    fn read_plenty() {
        test_read_write_single_threaded(10000, 3, 1);
    }

    #[test]
    fn multithreaded() {
        let done_writing_mut = Arc::new(Mutex::new(false));

        let read_count = Arc::new(AtomicU32::new(0));

        let write_ready_mutex = Arc::new(Mutex::new(0));
        let write_ready_cond = Arc::new(Condvar::new());

        let disk_queue = Arc::new(DiskQueue::new(TEST_DB_PATH));

        // Spawn 8 read threads
        let mut read_handles = vec![];
        for _ in 0..8 {
            let dq = disk_queue.clone();
            let write_ready_mutex = write_ready_mutex.clone();
            let write_ready_cond = write_ready_cond.clone();
            let done_writing_mut = done_writing_mut.clone();
            let read_count = read_count.clone();
            let h = std::thread::spawn(move || {
                {
                    let mut write_ready = write_ready_mutex.lock().unwrap();
                    while *write_ready < 8 {
                        write_ready = write_ready_cond.wait(write_ready).unwrap();
                    }
                }

                // TODO: Read threads is busy looping when there are no items
                // Perhaps use condition variable to wake up read thread?
                loop {
                    if let Some(_) = dq.dequeue() {
                        read_count.fetch_add(1, Ordering::Relaxed);
                    } else {
                        let done_writing = done_writing_mut.lock().unwrap();
                        if *done_writing {
                            break;
                        }
                    }
                }
            });
            read_handles.push(h);
        }

        // Spawn 8 write threads
        let mut write_handles = vec![];
        for tid in 0..8 {
            let dq = disk_queue.clone();
            let write_ready_mutex = write_ready_mutex.clone();
            let write_ready_cond = write_ready_cond.clone();
            let h = std::thread::spawn(move || {
                // Generate records
                let mut records = vec![];
                for i in 0..1000 {
                    let s = format!("record_t{}_{}", tid, i);
                    records.push(s.as_bytes().to_vec());
                }

                // Increment write ready
                {
                    let mut write_ready = write_ready_mutex.lock().unwrap();
                    *write_ready += 1;
                    if *write_ready >= 8 {
                        println!("All threads started");
                        write_ready_cond.notify_all();
                    } else {
                        while *write_ready < 8 {
                            write_ready = write_ready_cond
                                .wait(write_ready).unwrap();
                        }
                    }
                }

                println!("Write thread {} start enqueue-ing items", tid);

                // Start enqueue-ing items
                for record in records {
                    dq.enqueue(record);
                }

                println!("Write thread {} done", tid);
            });
            write_handles.push(h);
        }

        // Wait for all write threads to be ready
        {
            let mut write_ready = write_ready_mutex.lock().unwrap();
            while *write_ready < 8 {
                write_ready = write_ready_cond.wait(write_ready).unwrap();
            }
        }

        // Wait for write threads to finish and set done_writing
        for h in write_handles {
            h.join().unwrap();
        }
        {
            let mut done_writing = done_writing_mut.lock().unwrap();
            *done_writing = true;
        }

        // Wait for all read threads to finish
        for h in read_handles {
            h.join().unwrap();
        }

        assert_eq!(read_count.fetch_add(0, Ordering::Relaxed), 8000);
    }
}

