use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::ptr;
use std::sync::{Arc, RwLock};

use byteorder::{ByteOrder, BigEndian, ReadBytesExt, WriteBytesExt};
use memmap::MmapOptions;

const PAGE_SIZE: usize = 1024;
const PAGE_RECORD_POINTER_SIZE: usize = 4;
const MAGIC: u8 = 0xD7;

#[derive(Debug, PartialEq)]
struct Cursor {
    pageid: u64, // pageid starts from 1
    slotid: u16, // slotid starts from zero
}

// In-memory meta page representation
struct MetaPage {
    pub num_pages: u64,
    pub num_items: u64,
    pub read_cursor: Cursor,
    pub write_cursor: Cursor,

    // Pointer to mmap-ed memory
    mem: *const u8,
}

impl MetaPage {
    pub fn from_mmap_ptr(ptr: *const u8) -> Self {
        let num_pages: u64;
        let num_items: u64;
        let read_cursor: Cursor;
        let write_cursor: Cursor;
        unsafe {
            let mut offset = 0;

            // Read magic
            let magic = ptr.read();
            assert_eq!(magic, MAGIC);
            offset += 1;

            // Read num_pages
            let buf = ptr.offset(offset).cast::<[u8; 8]>().read();
            num_pages = BigEndian::read_u64(&buf);
            offset += 8;

            // Read num items
            let buf = ptr.offset(offset).cast::<[u8; 8]>().read();
            num_items = BigEndian::read_u64(&buf);
            offset += 8;

            // Read read_cursor
            let buf = ptr.offset(offset).cast::<[u8; 8]>().read();
            let pageid = BigEndian::read_u64(&buf);
            offset += 8;
            let buf = ptr.offset(offset).cast::<[u8; 2]>().read();
            let slotid = BigEndian::read_u16(&buf);
            offset += 2;
            read_cursor = Cursor { pageid, slotid };

            // Read write_cursor
            let buf = ptr.offset(offset).cast::<[u8; 8]>().read();
            let pageid = BigEndian::read_u64(&buf);
            offset += 8;
            let buf = ptr.offset(offset).cast::<[u8; 2]>().read();
            let slotid = BigEndian::read_u16(&buf);
            //offset += 2;
            write_cursor = Cursor { pageid, slotid };
        }

        Self {
            num_pages,
            num_items,
            read_cursor,
            write_cursor,
            mem: ptr,
        }
    }

    pub fn set_num_pages(&mut self, v: u64) {
        self.num_pages = v;
        unsafe {
            // TODO
        }
    }

    pub fn set_num_items(&mut self, v: u64) {
        // TODO
    }

    pub fn set_read_cursor(&mut self, v: Cursor) {
        // TODO
    }

    pub fn set_write_cursor(&mut self, v: Cursor) {
        // TODO
    }
}

// In-memory record page representation
struct RecordPage {
    pub records: Vec<Vec<u8>>,
    pub space_used: usize,

    // Pointer to mmap-ed memory
    mem: *const u8,
}

impl RecordPage {
    pub fn new() -> Self {
        Self {
            records: vec![],
            // magic (1 byte) + num of records (2 bytes)
            space_used: 1 + 2,
            mem: ptr::null(),
        }
    }

    pub fn from_mmap_ptr(ptr: *const u8) -> Self {
        // TODO
        Self {
            records: vec![],
            space_used: 3,
            mem: ptr,
        }
    }

    pub fn insert(&mut self, record: Vec<u8>) {
        assert!(self.space_used + self.record_space(&record) <= PAGE_SIZE);

        self.space_used += self.record_space(&record);
        self.records.push(record);
    }

    pub fn can_insert(&self, record: &Vec<u8>) -> bool {
        self.space_used + self.record_space(&record) <= PAGE_SIZE
    }

    #[inline(always)]
    fn record_space(&self, record: &Vec<u8>) -> usize {
        record.len() + 4 // the data and offset to the data
    }
}

// Helper structure only used for testing purposes
#[derive(Debug, PartialEq)]
struct Metadata {
    pub num_pages: u64,
    pub num_items: u64,
    pub read_cursor: Cursor,
    pub write_cursor: Cursor,
}

pub struct DiskQueue {
    num_pages: u64,
    num_items: u64,
    read_cursor: Cursor,
    write_cursor: Cursor,

    read_page: Arc<RwLock<RecordPage>>,
    write_page: Arc<RwLock<RecordPage>>,
}

impl DiskQueue {
    pub fn new(path: &str) -> Self {
        // TODO:
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

        // - Open the file, mmap-ing the first two pages
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let mut mmap = unsafe {
            MmapOptions::new()
                .len(2 * PAGE_SIZE)
                .map_mut(&file)
                .unwrap()
        };
        let meta_page_mem = mmap.as_ptr();
        let write_page_mem = unsafe {
            meta_page_mem.add(PAGE_SIZE)
        };
        
        let page = Arc::new(RwLock::new(RecordPage::new()));
        let read_page = page.clone();
        let write_page = page;

        let read_cursor = Cursor { pageid: 1, slotid: 0 };
        let write_cursor = Cursor { pageid: 1, slotid: 0 };

        Self {
            num_pages: 1,
            num_items: 0,
            read_cursor,
            write_cursor,
            read_page,
            write_page,
        }
    }

    pub fn enqueue(record: &[u8]) {
        // TODO
    }

    pub fn dequeue() -> Vec<u8> {
        // TODO
        vec![]
    }
}

fn buf_write_record_page(p: &RecordPage) -> [u8; PAGE_SIZE] {
    let sz = p.records.len();
    let record_top = 1; // After MAGIC
    let offset_top = PAGE_SIZE - 2; // page size - num of records (2 bytes)

    // Populate records and offsets buffer
    let mut records_buf: Vec<u8> = vec![];
    let offsets_buf_sz = sz * PAGE_RECORD_POINTER_SIZE;
    let mut offsets_buf: Vec<u8> = vec![0; offsets_buf_sz];
    for (slot, r) in p.records.iter().enumerate() {
        let record_off = (record_top + records_buf.len()) as u16;
        let record_len = r.len() as u16;
        records_buf.extend(r.iter());

        let off = offsets_buf_sz - (slot + 1) * PAGE_RECORD_POINTER_SIZE;
        BigEndian::write_u16(&mut offsets_buf[off..], record_off);
        BigEndian::write_u16(&mut offsets_buf[off+2..], record_len);
    }

    let mut buf: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
    buf[0] = MAGIC;
    buf[1..1+records_buf.len()].clone_from_slice(&records_buf);
    let empty_space = PAGE_SIZE - 1 - records_buf.len() - offsets_buf.len() - 2;
    let offset_bot = 1 + records_buf.len() + empty_space;
    buf[offset_bot..offset_bot+offsets_buf_sz].clone_from_slice(&offsets_buf);
    BigEndian::write_u16(&mut buf[offset_top..], sz as u16);
    
    buf
}

fn buf_read_record_page(buf: &[u8]) -> RecordPage {
    assert_eq!(buf.len(), PAGE_SIZE);
    assert_eq!(buf[0], MAGIC);

    let offset_top = PAGE_SIZE - 2; // page size - num of records (2 bytes)
    let sz = BigEndian::read_u16(&buf[offset_top..]);
    let mut page = RecordPage::new();
    for slot in 0..sz {
        let off: usize = offset_top - (slot as usize + 1) * PAGE_RECORD_POINTER_SIZE;
        let record_off = BigEndian::read_u16(&buf[off..]) as usize;
        let record_len = BigEndian::read_u16(&buf[off+2..]) as usize;
        let record = buf[record_off..record_off+record_len].to_vec();
        page.insert(record);
    }
    page
}

fn buf_write_metadata_page(m: &Metadata) -> [u8; PAGE_SIZE] {
    let mut buf: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
    buf[0] = MAGIC;
    BigEndian::write_u64(&mut buf[1..], m.num_pages);
    BigEndian::write_u64(&mut buf[9..], m.num_items);
    write_cursor(&mut buf[17..], &m.read_cursor);
    write_cursor(&mut buf[27..], &m.write_cursor);
    buf
}

fn buf_read_metadata_page(buf: &[u8]) -> Metadata {
    assert_eq!(buf.len(), PAGE_SIZE);
    
    let mut rdr = std::io::Cursor::new(buf);
    let magic = rdr.read_u8().unwrap();
    assert_eq!(magic, MAGIC);
    let num_pages = rdr.read_u64::<BigEndian>().unwrap();
    let num_items = rdr.read_u64::<BigEndian>().unwrap();
    let read_cursor_pageid = rdr.read_u64::<BigEndian>().unwrap();
    let read_cursor_slotid = rdr.read_u16::<BigEndian>().unwrap();
    let write_cursor_pageid = rdr.read_u64::<BigEndian>().unwrap();
    let write_cursor_slotid = rdr.read_u16::<BigEndian>().unwrap();

    Metadata {
        num_pages,
        num_items,
        read_cursor: Cursor { 
            pageid: read_cursor_pageid, 
            slotid: read_cursor_slotid,
        },
        write_cursor: Cursor { 
            pageid: write_cursor_pageid, 
            slotid: write_cursor_slotid,
        },
    }
}

fn write_cursor(buf: &mut [u8], cursor: &Cursor) {
    BigEndian::write_u64(buf, cursor.pageid);
    BigEndian::write_u16(&mut buf[8..], cursor.slotid);
}

fn read_cursor(buf: &[u8]) -> Cursor {
    // TODO
    Cursor { pageid: 0, slotid: 0 }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buf_write_and_read_metadata_page() {
        let m = Metadata {
            num_pages: 1,
            num_items: 3,
            read_cursor: Cursor { pageid: 1, slotid: 2 },
            write_cursor: Cursor { pageid: 1, slotid: 3 },
        };
        let page = buf_write_metadata_page(&m);
        let meta = buf_read_metadata_page(&page);

        assert_eq!(meta, m);
    }

    #[test]
    fn test_buf_write_and_read_record_page() {
        let records = vec![
            "https://www.google.com".as_bytes().to_vec(),
            "https://www.dexcode.com".as_bytes().to_vec(),
            "https://sahamee.com".as_bytes().to_vec(),
        ];
        let mut page = RecordPage::new();
        for record in records.iter() {
            assert_eq!(page.can_insert(&record), true);
            page.insert(record.clone());
        }

        let buf = buf_write_record_page(&page);
        let read_page = buf_read_record_page(&buf);
        assert_eq!(read_page.records.len(), 3);
        assert_eq!(read_page.records, records);

        let mut records_size = 0;
        for record in records.iter() {
            records_size += record.len();
        }
        assert_eq!(read_page.space_used, 3 + 3 * 4 + records_size);
    }
}

