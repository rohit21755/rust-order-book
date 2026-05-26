// Generates tonic server + client stubs from the canonical proto file at repo root.
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Workspace is at <repo>/matching-engine/. Proto file lives at <repo>/proto/.
    let proto = PathBuf::from("../../../proto/matching_engine.proto");
    let proto_dir = PathBuf::from("../../../proto");

    println!("cargo:rerun-if-changed={}", proto.display());
    println!("cargo:rerun-if-changed=build.rs");

    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&[proto], &[proto_dir])?;
    Ok(())
}
