use std::path::{Path, PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut protos: Vec<PathBuf> = std::fs::read_dir("proto")?
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            (path.extension()? == "proto").then_some(path)
        })
        .collect();
    protos.sort();

    for proto in &protos {
        println!("cargo:rerun-if-changed={}", proto.display());
    }

    let descriptors = protox::compile(&protos, ["proto"])?;
    let bytes = prost::Message::encode_to_vec(&descriptors);
    let out = Path::new(&std::env::var("OUT_DIR")?).join("signal_descriptors.bin");
    std::fs::write(out, bytes)?;
    Ok(())
}
