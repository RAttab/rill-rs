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
pub enum rill_error {}

#[allow(non_camel_case_types)]
pub enum rill_pairs {}

#[allow(non_camel_case_types)]
pub enum rill_acc {}

#[allow(non_camel_case_types)]
pub enum rill_query {}

#[link(name = "rill")]
#[allow(non_camel_case_types)]
extern "C" {
    pub fn rill_errno_thread() -> *const rill_error; // shim.c
    pub fn rill_strerror(err: *const rill_error, dst: *mut libc::c_char, len: usize) -> usize;

    pub fn rill_pairs_new(cap: libc::size_t) -> *mut rill_pairs;
    pub fn rill_pairs_free(pairs: *mut rill_pairs);
    pub fn rill_pairs_clear(pairs: *mut rill_pairs);
    pub fn rill_pairs_cap(pairs: *const rill_pairs) -> libc::size_t; // shim.c
    pub fn rill_pairs_len(pairs: *const rill_pairs) -> libc::size_t; // shim.c
    pub fn rill_pairs_get(pairs: *const rill_pairs, index: libc::size_t) -> *const rill_kv; //shim.c
    pub fn rill_pairs_push(
        pairs: *mut rill_pairs, key: rill_key_t, val: rill_val_t) -> *mut rill_pairs;
    pub fn rill_pairs_compact(pairs: *mut rill_pairs);

    pub fn rill_acc_open(dir: *const libc::c_char, cap: libc::size_t) -> *mut rill_acc;
    pub fn rill_acc_close(acc: *mut rill_acc);
    pub fn rill_acc_ingest(acc: *mut rill_acc, key: rill_key_t, val: rill_val_t);

    pub fn rill_rotate(dir: *const libc::c_char, now: rill_ts_t) -> bool;

    pub fn rill_query_open(dir: *const libc::c_char) -> *mut rill_query;
    pub fn rill_query_close(query: *mut rill_query);
    pub fn rill_query_key(
        query: *const rill_query, key: rill_key_t, out: *mut rill_pairs) -> *mut rill_pairs;
    pub fn rill_query_keys(
        query: *const rill_query,
        keys: *const rill_key_t, len: libc::size_t,
        out: *mut rill_pairs) -> *mut rill_pairs;
    pub fn rill_query_vals(
        query: *const rill_query,
        vals: *const rill_val_t, len: libc::size_t,
        out: *mut rill_pairs) -> *mut rill_pairs;

    pub fn rill_store_write(
        file: *const libc::c_char,
        ts: rill_ts_t, quant: usize,
        pairs: *const rill_pairs) -> bool;
}
