fn main() -> Result<(), Box<dyn std::error::Error>> {
    let document = storage_api::openapi();
    let json = serde_json::to_string_pretty(&document)?;
    let yaml = yaml_serde::to_string(&document)?;
    let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    std::fs::write(
        directory.join("openapi-storage-api.json"),
        format!("{json}\n"),
    )?;
    std::fs::write(directory.join("openapi-storage-api.yaml"), yaml)?;
    Ok(())
}
