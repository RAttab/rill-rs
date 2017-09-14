extern crate libc;

#[allow(non_camel_case_types)]
pub type rill_key_t = u64;

#[allow(non_camel_case_types)]
pub type rill_val_t = u64;

#[allow(non_camel_case_types)]
pub type rill_ts_t = u64;

#[repr(C)]
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
#[allow(non_camel_case_types)]
pub struct rill_kv {
    pub key: rill_key_t,
    pub val: rill_val_t,
}

#[allow(non_camel_case_types)]
pub enum rill_pairs {}

#[allow(non_camel_case_types)]
pub enum rill {}

#[link(name = "rill")]
#[allow(non_camel_case_types)]
extern "C" {
    pub fn rill_pairs_new(cap: libc::size_t) -> *mut rill_pairs;
    pub fn rill_pairs_free(pairs: *mut rill_pairs);
    pub fn rill_pairs_clear(pairs: *mut rill_pairs);
    pub fn rill_pairs_cap(pairs: *const rill_pairs) -> libc::size_t;
    pub fn rill_pairs_len(pairs: *const rill_pairs) -> libc::size_t;
    pub fn rill_pairs_get(pairs: *const rill_pairs, index: libc::size_t) -> *const rill_kv;
    pub fn rill_pairs_push(
        pairs: *mut rill_pairs, key: rill_key_t, val: rill_val_t) -> *mut rill_pairs;
    pub fn rill_pairs_compact(pairs: *mut rill_pairs);

    pub fn rill_open(dir: *const libc::c_char) -> *mut rill;
    pub fn rill_close(db: *mut rill);
    pub fn rill_ingest(db: *mut rill, key: rill_key_t, val: rill_val_t) -> bool;
    pub fn rill_rotate(db: *mut rill, now: rill_ts_t) -> bool;
    pub fn rill_query_key(
            db: *mut rill,
            keys: *const rill_key_t, len: libc::size_t,
            out: *mut rill_pairs) -> *mut rill_pairs;
    pub fn rill_query_val(
            db: *mut rill,
            vals: *const rill_val_t, len: libc::size_t,
            out: *mut rill_pairs) -> *mut rill_pairs;
}
