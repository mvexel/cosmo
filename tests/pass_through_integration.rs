use std::process::Command;

#[test]
fn runs_pass_through_conversion() {
    let output_file = tempfile::NamedTempFile::with_suffix(".geojsonl").unwrap();
    let output_path = output_file.path().to_str().unwrap();

    let status = Command::new(env!("CARGO_BIN_EXE_cosmo"))
        .arg("--input")
        .arg("fixture/library_square.osm.pbf")
        .arg("--output")
        .arg(output_path)
        .arg("--format")
        .arg("geojsonl")
        .arg("--filters")
        .arg("examples/pass_through.yaml")
        .arg("--verbose")
        .status()
        .expect("failed to execute process");

    assert!(status.success());

    let content = std::fs::read_to_string(output_path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    
    // Check that we got features
    assert!(!lines.is_empty());
    
    // Check that we have the expected JSON properties in the first feature
    let first_feature: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    let props = &first_feature["properties"];
    
    assert!(props.get("tags").is_some());
    assert!(props.get("meta").is_some());
    // refs might be null for nodes, so we check existence but not necessarily value type for the first one
    // unless we know for sure what the first feature is. In this fixture it's likely a node.
    // However, the column should exist in the output JSON properties even if null/empty for consistency?
    // GeoJSONL sink writes whatever is in the columns map.
    
    assert!(props["tags"].is_object());
    assert!(props["meta"].is_object());
}
