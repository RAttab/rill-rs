extern crate gcc;

use std::env;
use std::path::PathBuf;

fn main() {
    let base: String = env::var("CARGO_MANIFEST_DIR").unwrap();

    let src: PathBuf = [base.as_str(), "rill", "src"].iter().collect();
    let mut obj: Vec<String> =
        vec!("htable.c", "rng.c", "utils.c", "pairs.c", "store.c", "acc.c", "rotate.c", "query.c")
        .iter() .map(|&obj| {
            let mut path = src.clone();
            path.push(obj);
            path.to_str().unwrap().to_string()
        }).collect();

    // \todo This feels ridiculous and contrived
    let shim: PathBuf = [base.as_str(), "src", "shim.c"].iter().collect();
    obj.push(shim.to_str().unwrap().to_string());

    gcc::Build::new()
        .files(obj.iter())
        .include(src.to_str().unwrap())
        .flag_if_supported("-std=gnu11")
        .flag_if_supported("-fno-strict-aliasing")
        .define("_GNU_SOURCE", None)
        .static_flag(true)
        .opt_level(3)
        .warnings(true)
        .compile("rill");
}
