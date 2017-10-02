extern crate libc;

mod ffi;

use std::path::Path;

pub type Result<T> = std::result::Result<T, String>;

pub type Ts = ffi::rill_ts_t;
pub type Key = ffi::rill_key_t;
pub type Val = ffi::rill_val_t;
pub type KV = ffi::rill_kv;

// This function is kind-of insane. I really hope there's an easier
// way to do this...
fn err<T>() -> Result<T> {
    let err = unsafe{ ffi::rill_errno_thread() };

    let mut buf: [u8; 1024] = [0; 1024];
    let len = unsafe{
        let ptr = buf.as_mut_ptr() as *mut libc::c_char;
        let len = ffi::rill_strerror(err, ptr, buf.len());
        if len >= buf.len() { buf.len() } else { len + 1 }
    };

    match std::ffi::CStr::from_bytes_with_nul(&buf[0..len]) {
        Err(err) => Err(format!("invalid error string: {}", err)),
        Ok(msg) => match msg.to_str() {
            Err(err) => Err(format!("invalid error string: {}", err)),
            Ok(msg) =>Err(String::from(msg)),
        }
    }
}

// \todo Need to add iterator support.
pub struct Pairs { pairs: *mut ffi::rill_pairs }

impl Pairs {
    pub fn with_capacity(cap: usize) -> Result<Pairs> {
        let ptr = unsafe { ffi::rill_pairs_new(cap) };
        return if !ptr.is_null() { Ok(Pairs{ pairs: ptr }) } else { err() }
    }

    pub fn clear(&mut self) {
        unsafe { ffi::rill_pairs_clear(self.pairs) }
    }

    pub fn capacity(&self) -> usize {
        unsafe { ffi::rill_pairs_cap(self.pairs) }
    }

    pub fn len(&self) -> usize {
        unsafe { ffi::rill_pairs_len(self.pairs) }
    }

    pub fn get(&self, index: usize) -> &KV {
        unsafe { &*ffi::rill_pairs_get(self.pairs, index) }
    }

    pub fn push(&mut self, key: Key, val: Val) -> Result<()> {
        let result = unsafe { ffi::rill_pairs_push(self.pairs, key, val) };
        if result.is_null() { return err() };
        self.pairs = result;
        Ok(())
    }

    pub fn compact(&mut self) {
        unsafe { ffi::rill_pairs_compact(self.pairs) }
    }
}

impl Drop for Pairs {
    fn drop(&mut self) {
        unsafe { ffi::rill_pairs_free(self.pairs) }
    }
}

#[test]
fn test_pairs() {
    let mut pairs = Pairs::with_capacity(1).unwrap();

    pairs.push(10, 20).unwrap();
    pairs.push(20, 10).unwrap();
    pairs.push(10, 10).unwrap();

    pairs.compact();

    for i in 0..pairs.len() {
        let kv = pairs.get(i);
        println!("kv[{}]: key={}, len={}", i, kv.key, kv.val);
    }
}

// Pretty sure the UTF-8 checks between the path and the str
// conversions are redundant and just make things annoying to write.
fn path_to_c_str(path: &Path) -> Result<std::ffi::CString> {
    match path.to_str() {
        None => Err(format!("invalid dir path: '{:?}'", path)),
        Some(str_path) => match std::ffi::CString::new(str_path.as_bytes()) {
            Err(err) => Err(format!("invalid dir path: '{}': {}", str_path, err)),
            Ok(c_path) => Ok(c_path),
        },
    }
}


pub fn rotate(dir: &Path, now: Ts) -> Result<()> {
    let c_dir = path_to_c_str(dir)?;
    let ret = unsafe { ffi::rill_rotate(c_dir.as_ptr(), now) };
    return if ret { Ok(()) } else{ err() }
}

pub struct Acc { acc: *mut ffi::rill_acc }

impl Acc {
    pub fn new(dir: &Path, cap: usize) -> Result<Acc> {
        let c_dir = path_to_c_str(dir)?;
        let acc = unsafe { ffi::rill_acc_open(c_dir.as_ptr(), cap) };
        return if !acc.is_null() { Ok(Acc{acc: acc})} else { err() }
    }

    pub fn ingest(&mut self, key: Key, val: Val) {
        unsafe { ffi::rill_acc_ingest(self.acc, key, val) }
    }
}

impl Drop for Acc {
    fn drop(&mut self) {
        unsafe { ffi::rill_acc_close(self.acc) }
    }
}


pub struct Query { query: *mut ffi::rill_query }

impl Query {
    pub fn new(dir: &Path) -> Result<Query> {
        let c_dir = path_to_c_str(dir)?;
        let query = unsafe { ffi::rill_query_open(c_dir.as_ptr()) };
        return if !query.is_null() { Ok(Query{query: query}) } else { err() }
    }

    pub fn key(&self, key: Key, pairs: &mut Pairs) -> Result<()> {
        let result = unsafe { ffi::rill_query_key(self.query, key, pairs.pairs) };
        if result.is_null() { return err(); }
        pairs.pairs = result;
        Ok(())
    }

    // We pass in a &mut Pairs instead of returning as it allows to
    // pre-size the object size and to reuse the object across
    // multiple function calls.
    pub fn keys(&self, keys: &[Key], pairs: &mut Pairs) -> Result<()> {
        let result = unsafe {
            ffi::rill_query_keys(self.query, keys.as_ptr(), keys.len(), pairs.pairs)
        };
        if result.is_null() { return err(); }

        pairs.pairs = result;
        Ok(())
    }

    // \todo We only ever expect one val query during the lifetime of
    // the process so it's worth considering just returning a pairs
    // and not worrying too much about performance.
    pub fn vals(&self, vals: &[Val], pairs: &mut Pairs) -> Result<()> {
        let result = unsafe {
            ffi::rill_query_vals(self.query, vals.as_ptr(), vals.len(), pairs.pairs)
        };
        if result.is_null() { return err(); }

        pairs.pairs = result;
        Ok(())
    }
}

impl Drop for Query {
    fn drop(&mut self) {
        unsafe { ffi::rill_query_close(self.query) }
    }
}

#[test]
fn test_rotate_query() {
    let dir = Path::new("/tmp/rill-rs.rotate.test");
    let _ = std::fs::remove_dir_all(dir);

    {
        let mut acc = Acc::new(dir, 2).unwrap();

        acc.ingest(1, 10);
        rotate(dir, 1 * 60 * 60).unwrap();

        acc.ingest(2, 10);
        acc.ingest(1, 10);
        rotate(dir, 2 * 60 * 60).unwrap();

        acc.ingest(2, 10);
        acc.ingest(1, 20);
        rotate(dir, 3 * 60 * 60).unwrap();

        acc.ingest(1, 30);
    }

    let query = Query::new(dir).unwrap();

    {
        let mut pairs = Pairs::with_capacity(1).unwrap();
        query.key(2, &mut pairs).unwrap();
        assert_eq!(pairs.len(), 1);
        assert_eq!(*pairs.get(0), KV{key: 2, val: 10});
    }

    {
        let mut pairs = Pairs::with_capacity(1).unwrap();
        query.keys(&[1], &mut pairs).unwrap();
        assert_eq!(pairs.len(), 2);
        assert_eq!(*pairs.get(0), KV{key: 1, val: 10});
        assert_eq!(*pairs.get(1), KV{key: 1, val: 20});
    }

    {
        let mut pairs = Pairs::with_capacity(1).unwrap();
        query.keys(&[2, 3], &mut pairs).unwrap();
        assert_eq!(pairs.len(), 1);
        assert_eq!(*pairs.get(0), KV{key: 2, val: 10});
    }

    {
        let mut pairs = Pairs::with_capacity(1).unwrap();
        query.vals(&[10], &mut pairs).unwrap();
        assert_eq!(pairs.len(), 2);
        assert_eq!(*pairs.get(0), KV{key: 1, val: 10});
        assert_eq!(*pairs.get(1), KV{key: 2, val: 10});
    }

    {
        let mut pairs = Pairs::with_capacity(1).unwrap();
        query.vals(&[20, 30], &mut pairs).unwrap();
        assert_eq!(pairs.len(), 1);
        assert_eq!(*pairs.get(0), KV{key: 1, val: 20});
    }
}

pub struct Store { }

impl Store {
    pub fn write(dir: &Path, ts: Ts, quant: usize, pairs: &Pairs) -> Result<()> {
        let c_dir = path_to_c_str(dir)?;
        let ret = unsafe {ffi::rill_store_write(c_dir.as_ptr(), ts, quant, pairs.pairs) };
        return if ret { Ok(()) } else { err()  }
    }
}

#[test]
fn test_store() {
    let file = Path::new("/tmp/rill-rs.store.test");
    let _ = std::fs::remove_file(file);

    let mut pairs = Pairs::with_capacity(10).unwrap();
    pairs.push(2, 10).unwrap();
    pairs.push(3, 30).unwrap();
    pairs.push(2, 30).unwrap();
    pairs.push(1, 10).unwrap();
    pairs.push(2, 20).unwrap();
    Store::write(file, 100, 0, &pairs).unwrap();
}
