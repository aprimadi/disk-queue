use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ptr;
use std::sync::{Arc, Mutex, RwLock};

use crate::constant::PAGE_SIZE;

const RECPTR_SZ: usize = 4;
const MAGIC: u8 = 0xD7;

#[derive(Clone, Debug, PartialEq)]
pub struct Cursor {
    pub pageid: u64, // pageid starts from 1
    pub slotid: u16, // slotid starts from zero
}

// In-memory meta page representation
pub struct MetaPage {
    num_pages: u64,
    num_items: u64,
    read_cursor: Cursor,
    write_cursor: Cursor,

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
            num_pages = ptr.offset(offset).cast::<u64>().read();
            offset += 8;

            // Read num items
            num_items = ptr.offset(offset).cast::<u64>().read();
            offset += 8;

            // Read read_cursor
            let pageid = ptr.offset(offset).cast::<u64>().read();
            offset += 8;
            let slotid = ptr.offset(offset).cast::<u16>().read();
            offset += 2;
            read_cursor = Cursor { pageid, slotid };

            // Read write_cursor
            let pageid = ptr.offset(offset).cast::<u64>().read();
            offset += 8;
            let slotid = ptr.offset(offset).cast::<u16>().read();
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
    
    pub fn incr_num_pages(&mut self) {
        let num_pages = self.num_pages;
        self.set_num_pages(num_pages + 1);
    }
    
    pub fn incr_num_items(&mut self) {
        let num_items = self.num_items;
        self.set_num_items(num_items + 1);
    }
    
    pub fn get_num_pages(&self) -> u64 {
        self.num_pages
    }
    
    pub fn get_num_items(&self) -> u64 {
        self.num_items
    }
    
    pub fn get_read_cursor(&self) -> Cursor {
        self.read_cursor.clone()
    }
    
    pub fn get_write_cursor(&self) -> Cursor {
        self.write_cursor.clone()
    }

    pub fn set_num_pages(&mut self, v: u64) {
        self.num_pages = v;
        
        unsafe {
            (self.mem.offset(1) as *mut u64).write(v);
        }
    }

    pub fn set_num_items(&mut self, v: u64) {
        self.num_items = v;
        
        unsafe {
            (self.mem.offset(9) as *mut u64).write(v);
        }
    }

    pub fn set_read_cursor(&mut self, v: Cursor) {
        self.read_cursor = v.clone();
        
        unsafe {
            // Write pageid and slotid
            (self.mem.offset(17) as *mut u64).write(v.pageid);
            (self.mem.offset(25) as *mut u16).write(v.slotid);
        }
    }

    pub fn set_write_cursor(&mut self, v: Cursor) {
        self.write_cursor = v.clone();
        
        unsafe {
            (self.mem.offset(27) as *mut u64).write(v.pageid);
            (self.mem.offset(35) as *mut u16).write(v.slotid);
        }
    }
}

// In-memory record page representation
pub struct RecordPage {
    // Whether the record page is loaded from shared memory
    shared_mem: bool,
    
    // These fields aren't used when the page is loaded from shared memory.
    records: Vec<Vec<u8>>,
    space_used: u16,
    // Offset for new record, during insert
    write_offset: u16,

    // This protects reading/writing from mem
    rwlatch: Arc<RwLock<()>>,
    // Pointer to mmap-ed memory
    mem: *const u8,
}

impl RecordPage {
    pub fn new() -> Self {
        Self {
            shared_mem: false,
            records: vec![],
            // magic (1 byte) + num of records (2 bytes)
            space_used: 1 + 2,
            write_offset: 1,
            rwlatch: Arc::new(RwLock::new(())),
            mem: ptr::null(),
        }
    }
    
    pub fn from_file(file: Arc<Mutex<File>>, pageid: u64) -> Self {
        let mut file = file.lock().unwrap();
        let off = (pageid + 1) * PAGE_SIZE as u64;
        file.seek(SeekFrom::Start(off)).unwrap();
        let mut buf: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        file.read_exact(&mut buf).unwrap();
        let page = buf_read_record_page(&buf);
        page
    }

    pub fn from_mmap_ptr(rwlatch: Arc<RwLock<()>>, ptr: *const u8) -> Self {
        let mut space_used = 3;
        let mut write_offset = 1;

        unsafe {
            // Acquires read latch for reading from ptr
            let _ = rwlatch.read().unwrap();

            // Read magic
            let magic = ptr.read();
            assert_eq!(magic, MAGIC);

            // Read num records
            let nrecords = ptr
                .offset((PAGE_SIZE - 2) as isize)
                .cast::<u16>()
                .read();

            // Populate space used and write offset
            let recptr_sz = RECPTR_SZ as isize;
            let mut off = (PAGE_SIZE - 2) as isize;
            for _ in 0..nrecords {
                off -= recptr_sz;
                let rec_len = ptr.offset(off+2).cast::<u16>().read();
                space_used += rec_len + 4;
                write_offset += rec_len;
            }
        }
        
        Self {
            shared_mem: true,
            records: vec![], // Unused
            space_used,
            write_offset,
            rwlatch,
            mem: ptr,
        }
    }
    
    pub fn save(&self, file: Arc<Mutex<File>>, pageid: u64) {
        let mut file = file.lock().unwrap();
        let off = (pageid + 1) * PAGE_SIZE as u64;
        file.seek(SeekFrom::Start(off)).unwrap();
        let mut buf: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        unsafe {
            ptr::copy(self.mem, buf.as_mut_ptr(), PAGE_SIZE);
        }
        file.write(&buf).unwrap();
    }
    
    pub fn reset(&mut self) {
        let buf = empty_page_buf();
        unsafe {
            let _ = self.rwlatch.write().unwrap();
            ptr::copy(buf.as_ptr(), self.mem as *mut u8, PAGE_SIZE);
        }

        self.space_used = 3;
        self.write_offset = 1;
    }
    
    pub fn num_records(&self) -> usize {
        if self.shared_mem {
            let num_records;

            // Read num records
            unsafe {
                let _ = self.rwlatch.read().unwrap();
                num_records = self.mem
                    .offset((PAGE_SIZE - 2) as isize)
                    .cast::<u16>()
                    .read() as usize;
            }
            num_records
        } else {
            self.records.len()
        }
    }
    
    pub fn get_record(&self, slot: usize) -> Option<Vec<u8>> {
        println!("slot: {}", slot);
        if self.shared_mem {
            let record;
            unsafe {
                // Acquires read latch
                let _ = self.rwlatch.read().unwrap();

                // Read num records
                let num_records = self.mem.offset((PAGE_SIZE - 2) as isize)
                    .cast::<u16>()
                    .read() as usize;

                assert!(slot < num_records);

                // Read offset, size
                let off = (PAGE_SIZE - 2 - (slot + 1) * 4) as isize;
                let offset = self.mem.offset(off).cast::<u16>().read() as isize;
                let size = self.mem.offset(off+2).cast::<u16>().read() as usize;

                // Read record
                let buf = std::slice::from_raw_parts(self.mem.offset(offset), size);
                record = buf.to_vec();
            }
            Some(record)
        } else {
            self.records.get(slot).map(|x| x.clone())
        }
    }

    pub fn insert(&mut self, record: Vec<u8>) {
        assert!(self.space_used + self.record_space(&record) <= PAGE_SIZE as u16);
        assert!(self.shared_mem);

        unsafe {
            assert!(self.mem != ptr::null());

            // Acquires write latch
            let _ = self.rwlatch.write().unwrap();

            // Write num records
            let off = (PAGE_SIZE - 2) as isize;
            let num_records = self.mem.offset(off)
                .cast::<u16>()
                .read();
            (self.mem.offset(off).cast::<u16>() as *mut u16)
                .write(num_records + 1);

            // Write record pointer
            let off = (PAGE_SIZE - 2 - (num_records as usize + 1) * 4) as isize;
            (self.mem.offset(off).cast::<u16>() as *mut u16)
                .write(self.write_offset);
            (self.mem.offset(off+2).cast::<u16>() as *mut u16)
                .write(record.len() as u16);

            // Write record
            let off = self.write_offset as isize;
            std::ptr::copy(
                record.as_ptr(), 
                self.mem.offset(off) as *mut u8, 
                record.len()
            );
        }

        self.non_mmap_insert(record.clone());
    }

    pub fn is_shared_mem(&self) -> bool {
        self.shared_mem
    }
    
    fn non_mmap_insert(&mut self, record: Vec<u8>) {
        self.space_used += self.record_space(&record);
        self.write_offset += record.len() as u16;
    }

    pub fn can_insert(&self, record: &Vec<u8>) -> bool {
        assert_eq!(self.shared_mem, true);
        self.space_used + self.record_space(&record) <= PAGE_SIZE as u16
    }

    #[inline(always)]
    fn record_space(&self, record: &Vec<u8>) -> u16 {
        record.len() as u16 + 4 // the data and offset to the data
    }
}

// Helper structure only used for testing purposes
#[derive(Debug, PartialEq)]
pub struct Metadata {
    pub num_pages: u64,
    pub num_items: u64,
    pub read_cursor: Cursor,
    pub write_cursor: Cursor,
}


pub fn empty_page_buf() -> [u8; PAGE_SIZE] {
    let mut buf = [0; PAGE_SIZE];
    buf[0] = MAGIC;
    buf
}

/*
pub fn buf_write_record_page(p: &RecordPage) -> [u8; PAGE_SIZE] {
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
*/

fn buf_read_record_page(buf: &[u8]) -> RecordPage {
    assert_eq!(buf.len(), PAGE_SIZE);
    assert_eq!(buf[0], MAGIC);

    let ptr: *const u8 = buf.as_ptr();

    let offset_top = (PAGE_SIZE - 2) as isize; // page size - num of records (2 bytes)
    let mut page;
    unsafe {
        let sz = ptr.offset(offset_top).cast::<u16>().read();
        page = RecordPage::new();
        for slot in 0..sz {
            let off: isize = offset_top - (slot as isize + 1) * RECPTR_SZ as isize;
            let record_off = ptr.offset(off).cast::<u16>().read() as usize;
            let record_len = ptr.offset(off+2).cast::<u16>().read() as usize;
            let record = buf[record_off..record_off+record_len].to_vec();
            page.space_used += page.record_space(&record);
            page.write_offset += record.len() as u16;
            page.records.push(record);
        }
    }
    page
}

pub fn buf_write_metadata_page(m: &Metadata) -> [u8; PAGE_SIZE] {
    let mut buf: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
    buf[0] = MAGIC;
    let ptr: *mut u8 = buf.as_mut_ptr();
    unsafe {
        ptr.offset(1).cast::<u64>().write(m.num_pages);
        ptr.offset(9).cast::<u64>().write(m.num_items);
        ptr.offset(17).cast::<u64>().write(m.read_cursor.pageid);
        ptr.offset(25).cast::<u16>().write(m.read_cursor.slotid);
        ptr.offset(27).cast::<u64>().write(m.write_cursor.pageid);
        ptr.offset(35).cast::<u16>().write(m.write_cursor.slotid);
    }
    buf
}

