extern crate gcc;

use std::env;
use std::path::PathBuf;

fn main() {
    let base: String = env::var("CARGO_MANIFEST_DIR").unwrap();
    let src: PathBuf = [base.as_str(), "rill", "src"].iter().collect();

    let mut gcc = gcc::Config::new();
    gcc.include(src.to_str().unwrap());
    gcc.flag("-std=gnu11");
    gcc.flag("-fno-strict-aliasing");
    gcc.define("_GNU_SOURCE", None);
    gcc.opt_level(3);

    let files = [
        "htable.c", "rng.c", "utils.c", "pairs.c",
        "store.c", "acc.c", "rotate.c", "query.c"
    ];

    for file in &files {
        let mut path = src.clone();
        path.push(file);
        gcc.file(path);
    }

    {
        let shim: PathBuf = [base.as_str(), "src", "shim.c"].iter().collect();
        gcc.file(shim);
    }

    gcc.compile("librill.a");
}
