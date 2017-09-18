extern crate libc;

mod ffi;

pub type Result<T> = std::result::Result<T, String>;

pub type Ts = ffi::rill_ts_t;
pub type Key = ffi::rill_key_t;
pub type Val = ffi::rill_val_t;
pub type KV = ffi::rill_kv;

// \todo Need to add iterator support.
pub struct Pairs { pairs: *mut ffi::rill_pairs }

impl Pairs {
    pub fn new(cap: usize) -> Result<Pairs> {
        unsafe {
            let ptr = ffi::rill_pairs_new(cap);
            if ptr.is_null() { return Err("unable to allocate pairs".to_string()); }
            Ok(Pairs{ pairs: ptr })
        }
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
        unsafe {
            let result = ffi::rill_pairs_push(self.pairs, key, val);
            if result.is_null() { return Err("unable to push to pairs".to_string()); }
            self.pairs = result;
        }
        return Ok(())
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
    let mut pairs = Pairs::new(1).unwrap();

    pairs.push(10, 20).unwrap();
    pairs.push(20, 10).unwrap();
    pairs.push(10, 10).unwrap();

    pairs.compact();

    for i in 0..pairs.len() {
        let kv = pairs.get(i);
        println!("kv[{}]: key={}, len={}", i, kv.key, kv.val);
    }
}


pub fn rotate(dir: &str, now: Ts) -> Result<()> {
    let c_dir = match std::ffi::CString::new(dir.as_bytes()) {
        Ok(val) => val,
        Err(err) => return Err(format!("invalid dir path: '{}': {}", dir, err)),
    };

    let ret = unsafe { ffi::rill_rotate(c_dir.as_ptr(), now) };
    if !ret { return Err(format!("error occured while rotating '{}'", dir)) };
    return Ok(())
}

pub struct Acc { acc: *mut ffi::rill_acc }

impl Acc {
    pub fn new(dir: &str, cap: usize) -> Result<Acc> {
        let c_dir = match std::ffi::CString::new(dir.as_bytes()) {
            Ok(val) => val,
            Err(err) => return Err(format!("invalid dir path: '{}': {}", dir, err)),
        };

        let acc = unsafe { ffi::rill_acc_open(c_dir.as_ptr(), cap) };
        if acc.is_null() { return Err(format!("unable to open acc '{}'", dir)) }
        Ok(Acc{acc: acc})
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
    pub fn new(dir: &str) -> Result<Query> {
        let c_dir = match std::ffi::CString::new(dir.as_bytes()) {
            Ok(val) => val,
            Err(err) => return Err(format!("invalid dir path: '{}': {}", dir, err)),
        };

        let query = unsafe { ffi::rill_query_open(c_dir.as_ptr()) };
        if query.is_null() { return Err(format!("unable to open query '{}'", dir)) }
        Ok(Query{query: query})
    }

    // We pass in a &mut Pairs instead of returning as it allows to
    // pre-size the object size and to reuse the object across
    // multiple function calls.
    pub fn keys(&self, keys: &[Key], pairs: &mut Pairs) -> Result<()> {
        let result = unsafe {
            ffi::rill_query_key(self.query, keys.as_ptr(), keys.len(), pairs.pairs)
        };
        if result.is_null() { return Err(format!("unable to query keys: {:?}", keys)); }

        pairs.pairs = result;
        Ok(())
    }

    // \todo We only ever expect one val query during the lifetime of
    // the process so it's worth considering just returning a pairs
    // and not worrying too much about performance.
    pub fn vals(&self, vals: &[Val], pairs: &mut Pairs) -> Result<()> {
        let result = unsafe {
            ffi::rill_query_val(self.query, vals.as_ptr(), vals.len(), pairs.pairs)
        };
        if result.is_null() { return Err(format!("unable to query vals: {:?}", vals)); }

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
    let dir = "/tmp/rill-rs.test";
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
        let mut pairs = Pairs::new(1).unwrap();
        query.keys(&[1], &mut pairs).unwrap();
        assert_eq!(pairs.len(), 2);
        assert_eq!(*pairs.get(0), KV{key: 1, val: 10});
        assert_eq!(*pairs.get(1), KV{key: 1, val: 20});
    }

    {
        let mut pairs = Pairs::new(1).unwrap();
        query.keys(&[2, 3], &mut pairs).unwrap();
        assert_eq!(pairs.len(), 1);
        assert_eq!(*pairs.get(0), KV{key: 2, val: 10});
    }

    {
        let mut pairs = Pairs::new(1).unwrap();
        query.vals(&[10], &mut pairs).unwrap();
        assert_eq!(pairs.len(), 2);
        assert_eq!(*pairs.get(0), KV{key: 1, val: 10});
        assert_eq!(*pairs.get(1), KV{key: 2, val: 10});
    }

    {
        let mut pairs = Pairs::new(1).unwrap();
        query.vals(&[20, 30], &mut pairs).unwrap();
        assert_eq!(pairs.len(), 1);
        assert_eq!(*pairs.get(0), KV{key: 1, val: 20});
    }
}
