extern crate libc;

mod ffi;

use std::path::Path;
use std::os::unix::ffi::OsStrExt;

pub type Error = String;
pub type Result<T> = std::result::Result<T, Error>;

pub type Ts = ffi::rill_ts_t;
pub type Key = ffi::rill_key_t;
pub type Val = ffi::rill_val_t;
pub type KV = ffi::rill_kv;

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
        if !ptr.is_null() { Ok(Pairs{ pairs: ptr }) } else { err() }
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

    pub fn get(&self, index: usize) -> Result<KV> {
        let ptr = unsafe { ffi::rill_pairs_get(self.pairs, index) };
        if !ptr.is_null() {
            unsafe { Ok(KV{key: (*ptr).key, val: (*ptr).val}) }
        }
        else {
            err()
        }
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

fn path_to_c_str(path: &Path) -> Result<std::ffi::CString> {
    match std::ffi::CString::new(path.as_os_str().as_bytes()) {
            Err(err) => Err(format!("invalid dir path: '{:?}': {}", path, err)),
            Ok(c_path) => Ok(c_path),
    }
}


pub fn rotate(dir: &Path, now: Ts) -> Result<()> {
    let c_dir = path_to_c_str(dir)?;
    let ret = unsafe { ffi::rill_rotate(c_dir.as_ptr(), now) };
    if ret { Ok(()) } else{ err() }
}

pub struct Acc { acc: *mut ffi::rill_acc }

impl Acc {
    pub fn new(dir: &Path, cap: usize) -> Result<Acc> {
        let c_dir = path_to_c_str(dir)?;
        let acc = unsafe { ffi::rill_acc_open(c_dir.as_ptr(), cap) };
        if !acc.is_null() { Ok(Acc{acc: acc})} else { err() }
    }

    pub fn ingest(&mut self, key: Key, val: Val) {
        unsafe { ffi::rill_acc_ingest(self.acc, key, val) }
    }

    pub fn write(&mut self, file: &Path, ts: Ts) -> Result<()> {
        let c_file = path_to_c_str(file)?;
        let ret = unsafe { ffi::rill_acc_write(self.acc, c_file.as_ptr(), ts) };
        if ret { Ok(()) } else { err() }
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
        if !query.is_null() { Ok(Query{query: query}) } else { err() }
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

    pub fn all(&self) -> Result<Pairs> {
        let ptr = unsafe { ffi::rill_query_all(self.query)};
        if !ptr.is_null() { Ok(Pairs{ pairs: ptr }) } else { err() }
    }
}

impl Drop for Query {
    fn drop(&mut self) {
        unsafe { ffi::rill_query_close(self.query) }
    }
}

pub struct Store {
    store: *mut ffi::rill_store
}

impl Store {
    pub fn open(dir: &Path) -> Result<Store> {
        let c_dir = path_to_c_str(dir)?;
        let store = unsafe { ffi::rill_store_open(c_dir.as_ptr()) };
        if !store.is_null() { Ok(Store{store: store}) } else { err() }
    }

    pub fn write(dir: &Path, ts: Ts, quant: usize, pairs: &Pairs) -> Result<()> {
        let c_dir = path_to_c_str(dir)?;
        let ret = unsafe { ffi::rill_store_write(c_dir.as_ptr(), ts, quant, pairs.pairs) };
        if ret { Ok(()) } else { err() }
    }

    pub fn merge(out: &Path, ts: Ts, quant: usize, stores: &[Store]) -> Result<()> {
        let c_out = path_to_c_str(out)?;
        let c_list: Vec<_> = stores.iter().map(|s| s.store as *const ffi::rill_store).collect();

        let ret = unsafe {
            ffi::rill_store_merge(c_out.as_ptr(), ts, quant, c_list.as_ptr(), c_list.len())
        };
        if ret { Ok(()) } else { err() }
    }

    pub fn rm(&mut self) -> Result<()> {
        let ret = unsafe { ffi::rill_store_rm(self.store) };
        if ret { self.store = std::ptr::null_mut(); Ok(()) } else { err() }
    }

    pub fn key(&self, key: Key, pairs: &mut Pairs) -> Result<()> {
        let result = unsafe { ffi::rill_store_query_key(self.store, key, pairs.pairs) };
        if result.is_null() { return err(); }
        pairs.pairs = result;
        Ok(())
    }

    // We pass in a &mut Pairs instead of returning as it allows to
    // pre-size the object size and to reuse the object across
    // multiple function calls.
    pub fn keys(&self, keys: &[Key], pairs: &mut Pairs) -> Result<()> {
        let result = unsafe {
            ffi::rill_store_scan_keys(self.store, keys.as_ptr(), keys.len(), pairs.pairs)
        };
        if result.is_null() { return err(); }

        pairs.pairs = result;
        Ok(())
    }

    // \todo We only ever expect one val store during the lifetime of
    // the process so it's worth considering just returning a pairs
    // and not worrying too much about performance.
    pub fn vals(&self, vals: &[Val], pairs: &mut Pairs) -> Result<()> {
        let result = unsafe {
            ffi::rill_store_scan_vals(self.store, vals.as_ptr(), vals.len(), pairs.pairs)
        };
        if result.is_null() { return err(); }

        pairs.pairs = result;
        Ok(())
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        if !self.store.is_null() {
            unsafe { ffi::rill_store_close(self.store) }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_pairs() {
        let mut pairs = Pairs::with_capacity(1).unwrap();

        pairs.push(10, 20).unwrap();
        pairs.push(20, 10).unwrap();
        pairs.push(10, 10).unwrap();

        pairs.compact();

        for i in 0..pairs.len() {
            let kv = pairs.get(i).unwrap();
            println!("kv[{}]: key={}, len={}", i, kv.key, kv.val);
        }
    }


    #[test]
    fn test_rotate_query() {
        let dir = Path::new("/tmp/rill-rs.rotate.test");
        let _ = std::fs::remove_dir_all(dir);

        let acc_file = |name| {
            let mut pbuf = PathBuf::new();
            pbuf.push(dir);
            pbuf.push(format!("{}.rill", name));
            pbuf
        };

        {
            let mut acc = Acc::new(dir, 2).unwrap();

            acc.ingest(1, 10);
            acc.write(acc_file("1").as_path(), 1 * 60 * 60).unwrap();
            rotate(dir, 1 * 60 * 60).unwrap();

            acc.ingest(2, 10);
            acc.ingest(1, 10);
            acc.write(acc_file("2").as_path(), 2 * 60 * 60).unwrap();
            rotate(dir, 2 * 60 * 60).unwrap();

            acc.ingest(2, 10);
            acc.ingest(1, 20);
            acc.write(acc_file("3").as_path(), 3 * 60 * 60).unwrap();
            rotate(dir, 3 * 60 * 60).unwrap();

            acc.ingest(1, 30);
        }

        let query = Query::new(dir).unwrap();

        {
            let mut pairs = Pairs::with_capacity(1).unwrap();
            query.key(2, &mut pairs).unwrap();
            assert_eq!(pairs.len(), 1);
            assert_eq!(pairs.get(0).unwrap(), KV{key: 2, val: 10});
        }

        {
            let mut pairs = Pairs::with_capacity(1).unwrap();
            query.keys(&[1], &mut pairs).unwrap();
            assert_eq!(pairs.len(), 2);
            assert_eq!(pairs.get(0).unwrap(), KV{key: 1, val: 10});
            assert_eq!(pairs.get(1).unwrap(), KV{key: 1, val: 20});
        }

        {
            let mut pairs = Pairs::with_capacity(1).unwrap();
            query.keys(&[2, 3], &mut pairs).unwrap();
            assert_eq!(pairs.len(), 1);
            assert_eq!(pairs.get(0).unwrap(), KV{key: 2, val: 10});
        }

        {
            let mut pairs = Pairs::with_capacity(1).unwrap();
            query.vals(&[10], &mut pairs).unwrap();
            assert_eq!(pairs.len(), 2);
            assert_eq!(pairs.get(0).unwrap(), KV{key: 1, val: 10});
            assert_eq!(pairs.get(1).unwrap(), KV{key: 2, val: 10});
        }

        {
            let mut pairs = Pairs::with_capacity(1).unwrap();
            query.vals(&[20, 30], &mut pairs).unwrap();
            assert_eq!(pairs.len(), 1);
            assert_eq!(pairs.get(0).unwrap(), KV{key: 1, val: 20});
        }

        {
            let pairs = query.all().unwrap();
            assert_eq!(pairs.len(), 3);
            assert_eq!(pairs.get(0).unwrap(), KV{key: 1, val: 10});
            assert_eq!(pairs.get(1).unwrap(), KV{key: 1, val: 20});
            assert_eq!(pairs.get(2).unwrap(), KV{key: 2, val: 10});
        }
    }


    #[test]
    fn test_store() {
        let file_1 = Path::new("/tmp/rill-rs.store.test-1");
        {
            let _ = std::fs::remove_file(file_1);
            let mut pairs = Pairs::with_capacity(10).unwrap();
            pairs.push(2, 10).unwrap();
            pairs.push(3, 30).unwrap();
            pairs.push(2, 30).unwrap();
            pairs.push(1, 10).unwrap();
            pairs.push(2, 20).unwrap();
            Store::write(file_1, 100, 0, &pairs).unwrap();
        }

        let file_2 = Path::new("/tmp/rill-rs.store.test-2");
        {
            let _ = std::fs::remove_file(file_2);
            let mut pairs = Pairs::with_capacity(10).unwrap();
            pairs.push(2, 10).unwrap();
            pairs.push(3, 30).unwrap();
            pairs.push(4, 30).unwrap();
            pairs.push(2, 10).unwrap();
            pairs.push(3, 20).unwrap();
            Store::write(file_2, 100, 0, &pairs).unwrap();
        }

        let file_merge = Path::new("/tmp/rill-rs.store.test-merge");
        {
            let _ = std::fs::remove_file(file_merge);
            let mut to_merge = vec![
                Store::open(file_1).unwrap(),
                Store::open(file_2).unwrap()
            ];
            Store::merge(file_merge, 123, 60, to_merge.as_slice()).unwrap();
            to_merge[0].rm().unwrap();
            to_merge[1].rm().unwrap();
        }

        assert!(Store::open(file_1).is_err());
        assert!(Store::open(file_2).is_err());

        let store = Store::open(file_merge).unwrap();

        {
            let mut pairs = Pairs::with_capacity(1).unwrap();
            store.key(1, &mut pairs).unwrap();
            assert_eq!(pairs.len(), 1);
            assert_eq!(pairs.get(0).unwrap(), KV{key: 1, val: 10});

            pairs.clear();
            store.key(2, &mut pairs).unwrap();
            assert_eq!(pairs.len(), 3);
            assert_eq!(pairs.get(0).unwrap(), KV{key: 2, val: 10});
            assert_eq!(pairs.get(1).unwrap(), KV{key: 2, val: 20});
            assert_eq!(pairs.get(2).unwrap(), KV{key: 2, val: 30});

            pairs.clear();
            store.key(3, &mut pairs).unwrap();
            assert_eq!(pairs.len(), 2);
            assert_eq!(pairs.get(0).unwrap(), KV{key: 3, val: 20});
            assert_eq!(pairs.get(1).unwrap(), KV{key: 3, val: 30});

            pairs.clear();
            store.key(4, &mut pairs).unwrap();
            assert_eq!(pairs.len(), 1);
            assert_eq!(pairs.get(0).unwrap(), KV{key: 4, val: 30});
        }
    }
}
