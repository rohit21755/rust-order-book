// Generates Rust types from ../../proto/*.proto via prost-build.
use std::io::Result;
use std::path::PathBuf;

fn main() -> Result<()> {
    let proto_dir = PathBuf::from("../../proto");
    let protos = [
        proto_dir.join("order.proto"),
        proto_dir.join("trade.proto"),
        proto_dir.join("orderbook.proto"),
    ];

    for p in &protos {
        println!("cargo:rerun-if-changed={}", p.display());
    }
    println!("cargo:rerun-if-changed=build.rs");

    let mut config = prost_build::Config::new();
    config.bytes(["."]);
    config.compile_protos(&protos, &[proto_dir])?;
    Ok(())
}
