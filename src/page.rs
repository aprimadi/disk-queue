use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ptr;
use std::sync::{Arc, Mutex};

use byteorder::{ByteOrder, BigEndian};

use crate::constant::PAGE_SIZE;

const PAGE_RECORD_POINTER_SIZE: usize = 4;
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
            let mut buf: [u8; 8] = [0; 8];
            BigEndian::write_u64(&mut buf, v);
            let ptr = self.mem.offset(1).cast::<[u8; 8]>() as *mut [u8; 8];
            std::ptr::write(ptr, buf);
        }
    }

    pub fn set_num_items(&mut self, v: u64) {
        self.num_items = v;
        
        unsafe {
            let mut buf: [u8; 8] = [0; 8];
            BigEndian::write_u64(&mut buf, v);
            let ptr = self.mem.offset(9).cast::<[u8; 8]>() as *mut [u8; 8];
            std::ptr::write(ptr, buf);
        }
    }

    pub fn set_read_cursor(&mut self, v: Cursor) {
        self.read_cursor = v.clone();
        
        unsafe {
            // Write pageid
            let mut buf: [u8; 8] = [0; 8];
            BigEndian::write_u64(&mut buf, v.pageid);
            let ptr = self.mem.offset(17).cast::<[u8; 8]>() as *mut [u8; 8];
            std::ptr::write(ptr, buf);
            
            // Write slotid
            let mut buf: [u8; 2] = [0; 2];
            BigEndian::write_u16(&mut buf, v.slotid);
            let ptr = self.mem.offset(25).cast::<[u8; 2]>() as *mut [u8; 2];
            std::ptr::write(ptr, buf);
        }
    }

    pub fn set_write_cursor(&mut self, v: Cursor) {
        self.write_cursor = v.clone();
        
        unsafe {
            // Write pageid
            let mut buf: [u8; 8] = [0; 8];
            BigEndian::write_u64(&mut buf, v.pageid);
            let ptr = self.mem.offset(27).cast::<[u8; 8]>() as *mut [u8; 8];
            std::ptr::write(ptr, buf);
            
            // Write slotid
            let mut buf: [u8; 2] = [0; 2];
            BigEndian::write_u16(&mut buf, v.slotid);
            let ptr = self.mem.offset(35).cast::<[u8; 2]>() as *mut [u8; 2];
            std::ptr::write(ptr, buf);
        }
    }
}

// In-memory record page representation
pub struct RecordPage {
    records: Vec<Vec<u8>>,
    space_used: usize,
    // Offset for new record, during insert
    write_offset: u16,

    // Pointer to mmap-ed memory
    mem: *const u8,
}

impl RecordPage {
    pub fn new() -> Self {
        Self {
            records: vec![],
            // magic (1 byte) + num of records (2 bytes)
            space_used: 1 + 2,
            write_offset: 1,
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

    pub fn from_mmap_ptr(ptr: *const u8) -> Self {
        let mut records = vec![];
        
        unsafe {
            // Read magic
            let magic = ptr.read();
            assert_eq!(magic, MAGIC);
            
            // Read num records
            let buf = ptr.offset((PAGE_SIZE - 2) as isize).cast::<[u8; 2]>().read();
            let num_records = BigEndian::read_u16(&buf) as usize;
            
            // Read records
            for slot in 0..num_records {
                // Read record pointer
                let off = (PAGE_SIZE - 2 - (slot + 1) * 4) as isize;
                let buf = ptr.offset(off).cast::<[u8; 2]>().read();
                let offset = BigEndian::read_u16(&buf) as isize;
                let buf = ptr.offset(off+2).cast::<[u8; 2]>().read();
                let size = BigEndian::read_u16(&buf) as usize;
                
                // Read record
                let buf = std::slice::from_raw_parts(ptr.offset(offset), size);
                let record = buf.to_vec();
                records.push(record);
            }
        }
        
        // Calculate space used and write offset
        let (space_used, write_offset) = {
            let mut off = 1;
            let mut sz = 1 + 2; // magic + num records
            for record in records.iter() {
                off += record.len();
                sz += record.len();
                sz += 4; // record pointer
            }
            (sz, off as u16)
        };
        
        Self {
            records,
            space_used,
            write_offset,
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
        let page = RecordPage::new();
        let buf = buf_write_record_page(&page);
        unsafe {
            ptr::copy(buf.as_ptr(), self.mem as *mut u8, PAGE_SIZE);
        }

        self.records = vec![];
        self.space_used = 3;
        self.write_offset = 1;
    }
    
    pub fn num_records(&self) -> usize {
        self.records.len()
    }
    
    pub fn get_record(&self, slot: usize) -> Option<Vec<u8>> {
        self.records.get(slot).map(|x| x.clone())
    }

    pub fn insert(&mut self, record: Vec<u8>) {
        assert!(self.space_used + self.record_space(&record) <= PAGE_SIZE);
        assert!(self.mem != ptr::null());

        unsafe {
            // Write num records
            let mut buf: [u8; 2] = [0; 2];
            let num_records = (self.records.len() + 1) as u16;
            BigEndian::write_u16(&mut buf, num_records);
            let off = (PAGE_SIZE - 2) as isize;
            let ptr = self.mem.offset(off).cast::<[u8; 2]>() as *mut [u8; 2];
            std::ptr::write(ptr, buf);
            
            // Write record pointer
            let mut buf: [u8; 4] = [0; 4];
            BigEndian::write_u16(&mut buf, self.write_offset);
            BigEndian::write_u16(&mut buf[2..], record.len() as u16);
            let off = (PAGE_SIZE - 2 - (self.records.len() + 1) * 4) as isize;
            let ptr = self.mem.offset(off).cast::<[u8; 4]>() as *mut [u8; 4];
            std::ptr::write(ptr, buf);
            
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
    
    fn non_mmap_insert(&mut self, record: Vec<u8>) {
        self.space_used += self.record_space(&record);
        self.write_offset += record.len() as u16;
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
pub struct Metadata {
    pub num_pages: u64,
    pub num_items: u64,
    pub read_cursor: Cursor,
    pub write_cursor: Cursor,
}


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
        if record_off >= PAGE_SIZE || record_off + record_len >= PAGE_SIZE {
            println!("slot: {}", slot);
            println!("record_off: {}", record_off);
            println!("record_len: {}", record_len);
        }
        let record = buf[record_off..record_off+record_len].to_vec();
        page.non_mmap_insert(record);
    }
    page
}

pub fn buf_write_metadata_page(m: &Metadata) -> [u8; PAGE_SIZE] {
    let mut buf: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
    buf[0] = MAGIC;
    BigEndian::write_u64(&mut buf[1..], m.num_pages);
    BigEndian::write_u64(&mut buf[9..], m.num_items);
    write_cursor(&mut buf[17..], &m.read_cursor);
    write_cursor(&mut buf[27..], &m.write_cursor);
    buf
}

fn write_cursor(buf: &mut [u8], cursor: &Cursor) {
    BigEndian::write_u64(buf, cursor.pageid);
    BigEndian::write_u16(&mut buf[8..], cursor.slotid);
}

#[cfg(test)]
mod tests {
    use byteorder::ReadBytesExt;
    
    use super::*;

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
            page.non_mmap_insert(record.clone());
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
        assert_eq!(read_page.write_offset, (1 + records_size) as u16);
    }
}
