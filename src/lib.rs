use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

use memmap::{MmapMut, MmapOptions};

mod constant;
mod page;

use constant::PAGE_SIZE;
use page::{
    buf_write_metadata_page, buf_write_record_page,
    Cursor, Metadata, MetaPage, RecordPage
};

pub struct DiskQueue {
    file: Arc<Mutex<File>>,
    meta_page: Arc<RwLock<MetaPage>>,
    read_page: Arc<RwLock<RecordPage>>,
    write_page: Arc<RwLock<RecordPage>>,
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
            let page = RecordPage::new();
            let write_page_buf = buf_write_record_page(&page);
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
        
        let meta_page_mem = mmap.as_ptr();
        let write_page_mem = unsafe {
            meta_page_mem.add(PAGE_SIZE)
        };
        let file = Arc::new(Mutex::new(file));
        
        let meta_page = Arc::new(RwLock::new(MetaPage::from_mmap_ptr(meta_page_mem)));
        let write_page = Arc::new(RwLock::new(RecordPage::from_mmap_ptr(write_page_mem)));
        
        let read_page;
        {
            let meta_page = meta_page.write().unwrap();
            let num_pages = meta_page.get_num_pages();
            let read_cursor = meta_page.get_read_cursor();
            if read_cursor.pageid == num_pages {
                read_page = write_page.clone();
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
            _mmap: mmap,
        }
    }
    
    pub fn num_items(&self) -> u64 {
        let meta_page = self.meta_page.read().unwrap();
        meta_page.get_num_items()
    }

    pub fn enqueue(&mut self, record: Vec<u8>) {
        let mut meta_page = self.meta_page.write().unwrap();
        let mut write_page = self.write_page.write().unwrap();
        if write_page.can_insert(&record) {
            write_page.insert(record);
            
            meta_page.incr_num_items();
            let mut write_cursor = meta_page.get_write_cursor();
            write_cursor.slotid += 1;
            meta_page.set_write_cursor(write_cursor);
        } else {
            // Copy write page to a new page and reset write page
            let pageid = meta_page.get_num_pages();
            write_page.save(self.file.clone(), pageid);
            write_page.reset();
            
            write_page.insert(record);

            // Note that slotid is 1 since we just inserted a new record on 
            // the newly inserted page
            //
            // Also, we need to fix read cursor to point to a new page if it 
            // points to the same cursor as write cursor
            let mut write_cursor = meta_page.get_write_cursor();
            let mut read_cursor = meta_page.get_read_cursor();
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
            
            // A read page may point to the write page, in which case we need 
            // to load the newly written page and assign the read page to it.
            if Arc::ptr_eq(&self.read_page, &self.write_page) {
                let read_page = RecordPage::from_file(
                    self.file.clone(),
                    pageid,
                );
                self.read_page = Arc::new(RwLock::new(read_page));
            }
        }
    }

    pub fn dequeue(&mut self) -> Option<Vec<u8>> {
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
                None => {
                    println!("read_cursor: {:?}", read_cursor);
                    println!("write_cursor: {:?}", write_cursor);
                    panic!("Invariant violated");
                }
            }
            println!("read_cursor: {:?}", read_cursor);
            println!("write_cursor: {:?}", write_cursor);
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
            self.read_page = self.write_page.clone();
        }
        // TODO: There could be a problem with this
        if read_next_page {
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
        
            let mut queue = DiskQueue::new(TEST_DB_PATH);
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

            let mut queue = DiskQueue::new(TEST_DB_PATH);

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
                            println!("{}", String::from_utf8_lossy(&r));
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
        test_read_write_single_threaded(1000, 3, 1);
    }
}

