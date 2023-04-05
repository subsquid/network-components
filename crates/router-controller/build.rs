fn main() -> std::io::Result<()> {
    println!("cargo:rerun-if-changed=proto/messages.proto");
    prost_build::Config::new()
        .type_attribute(".", "#[derive(Eq, serde::Serialize, serde::Deserialize)]")
        .type_attribute("messages.Range", "#[derive(Copy, Ord, PartialOrd)]")
        .compile_protos(&["proto/messages.proto"], &["proto/"])?;
    Ok(())
}
