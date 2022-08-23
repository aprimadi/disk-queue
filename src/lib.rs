use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

use memmap::MmapOptions;

mod constant;
mod page;

use constant::PAGE_SIZE;
use page::{
    buf_write_metadata_page, buf_write_record_page,
    Cursor, Metadata, MetaPage, RecordPage
};

pub struct DiskQueue {
    // TODO: This needs to be protected by latch?
    file: Arc<Mutex<File>>,
    meta_page: Arc<RwLock<MetaPage>>,
    read_page: Arc<RwLock<RecordPage>>,
    write_page: Arc<RwLock<RecordPage>>,
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
        }
    }

    pub fn enqueue(&self, record: Vec<u8>) {
        let mut meta_page = self.meta_page.write().unwrap();
        let mut write_page = self.write_page.write().unwrap();
        if write_page.can_insert(&record) {
            write_page.insert(record);
            
            meta_page.incr_num_items();
            let mut write_cursor = meta_page.get_write_cursor();
            write_cursor.slotid += 1;
            meta_page.set_write_cursor(write_cursor);
        } else {
            // TODO
            // - Copy write page to a new page
            // - Reset write page
        }
    }

    pub fn dequeue(&self) -> Option<Vec<u8>> {
        let mut meta_page = self.meta_page.write().unwrap();
        let read_page = self.read_page.read().unwrap();
        
        let num_pages = meta_page.get_num_pages();
        let num_records = read_page.num_records();
        let mut read_cursor = meta_page.get_read_cursor();
        let write_cursor = meta_page.get_write_cursor();
        
        if read_cursor == write_cursor {
            return None;
        }
        
        let record = read_page.get_record(read_cursor.slotid as usize);
        if read_cursor.slotid + 1 < num_records as u16 {
            read_cursor.slotid += 1;
            meta_page.set_read_cursor(read_cursor);
        } else {
            read_cursor.pageid += 1;
            read_cursor.slotid = 0;
            meta_page.set_read_cursor(read_cursor.clone());
            
            // TODO: Read the next page
            assert!(read_cursor.pageid <= num_pages);
        }
        
        Some(record)
    }
}
