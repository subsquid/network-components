fn main() -> std::io::Result<()> {
    println!("cargo:rerun-if-changed=proto/messages.proto");
    prost_build::Config::new()
        .type_attribute(".", "#[derive(Eq, serde::Serialize, serde::Deserialize)]")
        .type_attribute("messages.Range", "#[derive(Copy, Ord, PartialOrd)]")
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(&["proto/messages.proto"], &["proto/"])?;
    Ok(())
}
