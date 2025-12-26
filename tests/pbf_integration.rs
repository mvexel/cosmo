use std::path::{Path, PathBuf};
use std::process::Command;

fn fixture_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixture")
        .join("library_square.osm.pbf")
}

fn write_temp_filters(contents: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    path.push(format!("cosmo_filters_{pid}_{nanos}.yaml"));
    std::fs::write(&path, contents).expect("write filters yaml");
    path
}

fn run_cosmo(filters_yaml: &str) -> Vec<String> {
    let filters_path = write_temp_filters(filters_yaml);
    let exe = env!("CARGO_BIN_EXE_cosmo");

    let output = Command::new(exe)
        .arg("--input")
        .arg(fixture_path())
        .arg("--output")
        .arg("-")
        .arg("--format")
        .arg("geojsonl")
        .arg("--filters")
        .arg(&filters_path)
        .arg("--node-cache-mode")
        .arg("memory")
        .output()
        .expect("run cosmo");

    let _ = std::fs::remove_file(&filters_path);

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("cosmo failed: {}", stderr);
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.to_string())
        .collect()
}

fn run_cosmo_to_file(filters_yaml: &str, format: &str, output_path: &Path) {
    let filters_path = write_temp_filters(filters_yaml);
    let exe = env!("CARGO_BIN_EXE_cosmo");

    let output = Command::new(exe)
        .arg("--input")
        .arg(fixture_path())
        .arg("--output")
        .arg(output_path)
        .arg("--format")
        .arg(format)
        .arg("--filters")
        .arg(&filters_path)
        .arg("--node-cache-mode")
        .arg("memory")
        .output()
        .expect("run cosmo");

    let _ = std::fs::remove_file(&filters_path);

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("cosmo failed: {}", stderr);
    }
}

fn run_cosmo_expect_failure(filters_yaml: &str, args: &[&str]) -> String {
    let filters_path = write_temp_filters(filters_yaml);
    let exe = env!("CARGO_BIN_EXE_cosmo");

    let output = Command::new(exe)
        .arg("--input")
        .arg(fixture_path())
        .arg("--output")
        .arg("-")
        .arg("--format")
        .arg("geojsonl")
        .arg("--filters")
        .arg(&filters_path)
        .args(args)
        .output()
        .expect("run cosmo");

    let _ = std::fs::remove_file(&filters_path);

    assert!(!output.status.success(), "expected failure");
    String::from_utf8_lossy(&output.stderr).to_string()
}

#[test]
fn extracts_tree_nodes() {
    let filters = r#"
tables:
  trees:
    filter:
      tag: "natural"
      value: "tree"
    geometry:
      node: true
      way: false
      relation: false
    columns:
      - name: "id"
        source: "meta:id"
        type: "integer"
"#;
    let lines = run_cosmo(filters);
    assert_eq!(lines.len(), 261);
}

#[test]
fn extracts_lanes_tagged_ways() {
    let filters = r#"
tables:
  lanes:
    filter:
      tag: "lanes"
    geometry:
      node: false
      way: "linestring"
      relation: false
    columns:
      - name: "lanes"
        source: "tag:lanes"
        type: "string"
"#;
    let lines = run_cosmo(filters);
    assert_eq!(lines.len(), 49);
}

#[test]
fn writes_parquet_output() {
    let filters = r#"
tables:
  lanes:
    filter:
      tag: "lanes"
    geometry:
      node: false
      way: "linestring"
      relation: false
    columns:
      - name: "lanes"
        source: "tag:lanes"
        type: "string"
"#;
    let mut output_path = std::env::temp_dir();
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    output_path.push(format!("cosmo_test_{pid}_{nanos}.parquet"));

    run_cosmo_to_file(filters, "parquet", &output_path);

    let metadata = std::fs::metadata(&output_path).expect("parquet exists");
    assert!(metadata.len() > 0, "parquet output should not be empty");
    let _ = std::fs::remove_file(&output_path);
}

#[test]
fn mmap_cache_errors_on_small_size() {
    let filters = r#"
tables:
  lanes:
    filter:
      tag: "lanes"
    geometry:
      node: false
      way: "linestring"
      relation: false
    columns:
      - name: "lanes"
        source: "tag:lanes"
        type: "string"
"#;
    let stderr = run_cosmo_expect_failure(
        filters,
        &[
            "--node-cache-mode",
            "mmap",
            "--node-cache-max-nodes",
            "1000",
        ],
    );
    assert!(
        stderr.contains("exceeds node_cache_max_nodes"),
        "unexpected stderr: {stderr}"
    );
}
