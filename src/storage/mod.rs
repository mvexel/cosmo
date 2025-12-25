use anyhow::{Context, Result, anyhow};
use memmap2::{Mmap, MmapMut};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::path::Path;
use tempfile::NamedTempFile;

// 8 bytes per node: 4 bytes lon (i32), 4 bytes lat (i32)
// Stored as fixed precision integers (deg * 10^7)
const NODE_SIZE: usize = 8;
const SCALE_FACTOR: f64 = 10_000_000.0;

pub struct NodeStoreWriter {
    inner: NodeStoreWriterImpl,
}

pub struct NodeStoreReader {
    inner: NodeStoreReaderImpl,
}

enum NodeStoreWriterImpl {
    Sparse(SparseNodeStoreWriter),
    Dense(DenseNodeStoreWriter),
    Memory(MemoryNodeStore),
}

enum NodeStoreReaderImpl {
    Sparse(SparseNodeStoreReader),
    Dense(DenseNodeStoreReader),
    Memory(MemoryNodeStore),
}

struct SparseNodeStoreWriter {
    /// Entries stored as (node_id, packed_coords) - appended in order
    entries: Vec<(u64, i64)>,
}

struct SparseNodeStoreReader {
    /// Sorted by node_id for binary search
    entries: Vec<(u64, i64)>,
}

struct DenseNodeStoreWriter {
    mmap: MmapMut,
    max_nodes: u64,
    /// If Some, file is automatically deleted when this struct is dropped
    _temp_file: Option<NamedTempFile>,
}

struct DenseNodeStoreReader {
    mmap: Mmap,
    max_nodes: u64,
    /// If Some, file is automatically deleted when this struct is dropped
    _temp_file: Option<NamedTempFile>,
}

#[derive(Clone)]
struct MemoryNodeStore {
    nodes: HashMap<u64, (i32, i32)>,
}

impl NodeStoreWriter {
    /// Create a sparse node store (sorted array, efficient for extracts).
    pub fn new_sparse() -> Self {
        Self {
            inner: NodeStoreWriterImpl::Sparse(SparseNodeStoreWriter {
                entries: Vec::new(),
            }),
        }
    }

    /// Create a dense node store backed by a memory-mapped file at the given path.
    /// The file is NOT automatically deleted - caller is responsible for cleanup.
    pub fn new_dense<P: AsRef<Path>>(path: P, max_nodes: u64) -> Result<Self> {
        let path = path.as_ref();

        // Open or create the file
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .context("Failed to open node store file")?;

        // Set file length to max size (relying on sparse files)
        let file_size = max_nodes
            .checked_mul(NODE_SIZE as u64)
            .context("Node cache size overflow")?;
        file.set_len(file_size)
            .context("Failed to set node store file length")?;

        // Map the file
        let mmap = unsafe { MmapMut::map_mut(&file).context("Failed to map node store file")? };

        Ok(Self {
            inner: NodeStoreWriterImpl::Dense(DenseNodeStoreWriter {
                mmap,
                max_nodes,
                _temp_file: None,
            }),
        })
    }

    /// Create a dense node store backed by a temporary memory-mapped file.
    /// The file is automatically deleted when this store (and its reader) are dropped.
    pub fn new_dense_temp(max_nodes: u64) -> Result<Self> {
        // Create temp file
        let temp_file = NamedTempFile::new().context("Failed to create temporary node cache file")?;

        // Set file length to max size (relying on sparse files)
        let file_size = max_nodes
            .checked_mul(NODE_SIZE as u64)
            .context("Node cache size overflow")?;
        temp_file
            .as_file()
            .set_len(file_size)
            .context("Failed to set node store file length")?;

        // Map the file
        let mmap = unsafe {
            MmapMut::map_mut(temp_file.as_file()).context("Failed to map node store file")?
        };

        Ok(Self {
            inner: NodeStoreWriterImpl::Dense(DenseNodeStoreWriter {
                mmap,
                max_nodes,
                _temp_file: Some(temp_file),
            }),
        })
    }

    pub fn new_memory() -> Self {
        Self {
            inner: NodeStoreWriterImpl::Memory(MemoryNodeStore {
                nodes: HashMap::new(),
            }),
        }
    }

    pub fn put(&mut self, id: u64, lat: f64, lon: f64) -> Result<()> {
        match &mut self.inner {
            NodeStoreWriterImpl::Sparse(store) => store.put(id, lat, lon),
            NodeStoreWriterImpl::Dense(store) => store.put(id, lat, lon),
            NodeStoreWriterImpl::Memory(store) => store.put(id, lat, lon),
        }
    }

    pub fn finalize(self) -> Result<NodeStoreReader> {
        match self.inner {
            NodeStoreWriterImpl::Sparse(store) => store.finalize(),
            NodeStoreWriterImpl::Dense(store) => store.finalize(),
            NodeStoreWriterImpl::Memory(store) => Ok(NodeStoreReader {
                inner: NodeStoreReaderImpl::Memory(store),
            }),
        }
    }
}

impl NodeStoreReader {
    pub fn get(&self, id: u64) -> Option<(f64, f64)> {
        match &self.inner {
            NodeStoreReaderImpl::Sparse(store) => store.get(id),
            NodeStoreReaderImpl::Dense(store) => store.get(id),
            NodeStoreReaderImpl::Memory(store) => store.get(id),
        }
    }
}

/// Pack lat/lon into a single i64 for sparse storage
fn pack_coords(lat: f64, lon: f64) -> i64 {
    let lat_fixed = (lat * SCALE_FACTOR) as i32;
    let lon_fixed = (lon * SCALE_FACTOR) as i32;
    ((lon_fixed as i64) << 32) | ((lat_fixed as i64) & 0xFFFFFFFF)
}

/// Unpack i64 back to (lon, lat)
fn unpack_coords(packed: i64) -> (f64, f64) {
    let lon_fixed = (packed >> 32) as i32;
    let lat_fixed = packed as i32;
    (
        lon_fixed as f64 / SCALE_FACTOR,
        lat_fixed as f64 / SCALE_FACTOR,
    )
}

impl SparseNodeStoreWriter {
    fn put(&mut self, id: u64, lat: f64, lon: f64) -> Result<()> {
        let packed = pack_coords(lat, lon);
        self.entries.push((id, packed));
        Ok(())
    }

    fn finalize(mut self) -> Result<NodeStoreReader> {
        // OSM data is pre-sorted, but verify and sort if needed
        let is_sorted = self.entries.windows(2).all(|w| w[0].0 <= w[1].0);
        if !is_sorted {
            self.entries.sort_by_key(|(id, _)| *id);
        }

        Ok(NodeStoreReader {
            inner: NodeStoreReaderImpl::Sparse(SparseNodeStoreReader {
                entries: self.entries,
            }),
        })
    }
}

impl SparseNodeStoreReader {
    fn get(&self, id: u64) -> Option<(f64, f64)> {
        self.entries
            .binary_search_by_key(&id, |(node_id, _)| *node_id)
            .ok()
            .map(|idx| unpack_coords(self.entries[idx].1))
    }
}

impl DenseNodeStoreWriter {
    fn put(&mut self, id: u64, lat: f64, lon: f64) -> Result<()> {
        if id >= self.max_nodes {
            return Err(anyhow!(
                "node id {id} exceeds node_cache_max_nodes ({}); increase --node-cache-max-nodes or use --node-cache-mode memory",
                self.max_nodes
            ));
        }

        let offset = (id as usize) * NODE_SIZE;

        // Convert to fixed precision i32
        let lat_fixed = (lat * SCALE_FACTOR) as i32;
        let lon_fixed = (lon * SCALE_FACTOR) as i32;

        let data = &mut self.mmap[offset..offset + NODE_SIZE];
        data[0..4].copy_from_slice(&lon_fixed.to_le_bytes());
        data[4..8].copy_from_slice(&lat_fixed.to_le_bytes());
        Ok(())
    }

    fn finalize(self) -> Result<NodeStoreReader> {
        let mmap = self
            .mmap
            .make_read_only()
            .context("Failed to set node store to read-only")?;
        Ok(NodeStoreReader {
            inner: NodeStoreReaderImpl::Dense(DenseNodeStoreReader {
                mmap,
                max_nodes: self.max_nodes,
                _temp_file: self._temp_file, // Pass ownership for cleanup on drop
            }),
        })
    }
}

impl DenseNodeStoreReader {
    fn get(&self, id: u64) -> Option<(f64, f64)> {
        if id >= self.max_nodes {
            return None;
        }

        let offset = (id as usize) * NODE_SIZE;
        let data = &self.mmap[offset..offset + NODE_SIZE];

        Some(decode_coords(data))
    }
}

impl MemoryNodeStore {
    fn put(&mut self, id: u64, lat: f64, lon: f64) -> Result<()> {
        let lat_fixed = (lat * SCALE_FACTOR) as i32;
        let lon_fixed = (lon * SCALE_FACTOR) as i32;
        self.nodes.insert(id, (lon_fixed, lat_fixed));
        Ok(())
    }

    fn get(&self, id: u64) -> Option<(f64, f64)> {
        let (lon_fixed, lat_fixed) = self.nodes.get(&id)?;
        Some((
            *lon_fixed as f64 / SCALE_FACTOR,
            *lat_fixed as f64 / SCALE_FACTOR,
        ))
    }
}

fn decode_coords(data: &[u8]) -> (f64, f64) {
    let lon_bytes: [u8; 4] = data[0..4].try_into().unwrap();
    let lat_bytes: [u8; 4] = data[4..8].try_into().unwrap();

    let lon_fixed = i32::from_le_bytes(lon_bytes);
    let lat_fixed = i32::from_le_bytes(lat_bytes);

    let lon = lon_fixed as f64 / SCALE_FACTOR;
    let lat = lat_fixed as f64 / SCALE_FACTOR;

    (lon, lat)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ============================================
    // Coordinate encoding/decoding precision tests
    // ============================================

    #[test]
    fn encode_decode_preserves_precision_to_7_decimal_places() {
        // OSM coordinates have 7 decimal places of precision
        let lat = 51.5073509;
        let lon = -0.1277583;

        let lat_fixed = (lat * SCALE_FACTOR) as i32;
        let lon_fixed = (lon * SCALE_FACTOR) as i32;

        let lat_decoded = lat_fixed as f64 / SCALE_FACTOR;
        let lon_decoded = lon_fixed as f64 / SCALE_FACTOR;

        // Should be accurate to 7 decimal places (1e-7)
        assert!((lat - lat_decoded).abs() < 1e-7);
        assert!((lon - lon_decoded).abs() < 1e-7);
    }

    #[test]
    fn decode_coords_correctly_parses_bytes() {
        // Create known fixed-point values
        let lon_fixed: i32 = -1277583; // -0.1277583 * 10^7
        let lat_fixed: i32 = 515073509; // 51.5073509 * 10^7

        let mut data = [0u8; 8];
        data[0..4].copy_from_slice(&lon_fixed.to_le_bytes());
        data[4..8].copy_from_slice(&lat_fixed.to_le_bytes());

        let (lon, lat) = decode_coords(&data);

        assert!((lon - (-0.1277583)).abs() < 1e-7);
        assert!((lat - 51.5073509).abs() < 1e-7);
    }

    #[test]
    fn handles_extreme_coordinates() {
        // Maximum valid lat/lon
        let max_lat = 90.0;
        let max_lon = 180.0;
        let min_lat = -90.0;
        let min_lon = -180.0;

        // Verify these fit in i32 after scaling
        let max_lat_fixed = (max_lat * SCALE_FACTOR) as i32;
        let max_lon_fixed = (max_lon * SCALE_FACTOR) as i32;
        let min_lat_fixed = (min_lat * SCALE_FACTOR) as i32;
        let min_lon_fixed = (min_lon * SCALE_FACTOR) as i32;

        assert_eq!(max_lat_fixed, 900_000_000);
        assert_eq!(max_lon_fixed, 1_800_000_000);
        assert_eq!(min_lat_fixed, -900_000_000);
        assert_eq!(min_lon_fixed, -1_800_000_000);

        // Verify roundtrip
        assert!((max_lat_fixed as f64 / SCALE_FACTOR - max_lat).abs() < 1e-7);
        assert!((max_lon_fixed as f64 / SCALE_FACTOR - max_lon).abs() < 1e-7);
    }

    #[test]
    fn handles_zero_coordinates() {
        let lat = 0.0;
        let lon = 0.0;

        let lat_fixed = (lat * SCALE_FACTOR) as i32;
        let lon_fixed = (lon * SCALE_FACTOR) as i32;

        assert_eq!(lat_fixed, 0);
        assert_eq!(lon_fixed, 0);

        let mut data = [0u8; 8];
        data[0..4].copy_from_slice(&lon_fixed.to_le_bytes());
        data[4..8].copy_from_slice(&lat_fixed.to_le_bytes());

        let (decoded_lon, decoded_lat) = decode_coords(&data);
        assert_eq!(decoded_lon, 0.0);
        assert_eq!(decoded_lat, 0.0);
    }

    // ============================================
    // Memory node store tests
    // ============================================

    #[test]
    fn memory_store_put_and_get() {
        let mut writer = NodeStoreWriter::new_memory();
        writer.put(1, 51.5, -0.1).unwrap();
        writer.put(2, 40.7, -74.0).unwrap();

        let reader = writer.finalize().unwrap();

        let (lon, lat) = reader.get(1).unwrap();
        assert!((lat - 51.5).abs() < 1e-7);
        assert!((lon - (-0.1)).abs() < 1e-7);

        let (lon, lat) = reader.get(2).unwrap();
        assert!((lat - 40.7).abs() < 1e-7);
        assert!((lon - (-74.0)).abs() < 1e-7);
    }

    #[test]
    fn memory_store_returns_none_for_missing_node() {
        let writer = NodeStoreWriter::new_memory();
        let reader = writer.finalize().unwrap();

        assert!(reader.get(999).is_none());
    }

    #[test]
    fn memory_store_overwrites_existing_node() {
        let mut writer = NodeStoreWriter::new_memory();
        writer.put(1, 51.5, -0.1).unwrap();
        writer.put(1, 40.7, -74.0).unwrap(); // Overwrite

        let reader = writer.finalize().unwrap();

        let (lon, lat) = reader.get(1).unwrap();
        assert!((lat - 40.7).abs() < 1e-7);
        assert!((lon - (-74.0)).abs() < 1e-7);
    }

    #[test]
    fn memory_store_handles_large_node_ids() {
        let mut writer = NodeStoreWriter::new_memory();
        let large_id = u64::MAX - 1;
        writer.put(large_id, 51.5, -0.1).unwrap();

        let reader = writer.finalize().unwrap();

        let (lon, lat) = reader.get(large_id).unwrap();
        assert!((lat - 51.5).abs() < 1e-7);
        assert!((lon - (-0.1)).abs() < 1e-7);
    }

    // ============================================
    // Sparse node store tests
    // ============================================

    #[test]
    fn sparse_store_put_and_get() {
        let mut writer = NodeStoreWriter::new_sparse();
        writer.put(1, 51.5, -0.1).unwrap();
        writer.put(2, 40.7, -74.0).unwrap();

        let reader = writer.finalize().unwrap();

        let (lon, lat) = reader.get(1).unwrap();
        assert!((lat - 51.5).abs() < 1e-7);
        assert!((lon - (-0.1)).abs() < 1e-7);

        let (lon, lat) = reader.get(2).unwrap();
        assert!((lat - 40.7).abs() < 1e-7);
        assert!((lon - (-74.0)).abs() < 1e-7);
    }

    #[test]
    fn sparse_store_returns_none_for_missing_node() {
        let writer = NodeStoreWriter::new_sparse();
        let reader = writer.finalize().unwrap();

        assert!(reader.get(999).is_none());
    }

    #[test]
    fn sparse_store_handles_unsorted_input() {
        let mut writer = NodeStoreWriter::new_sparse();
        // Insert out of order
        writer.put(5, 51.5, -0.1).unwrap();
        writer.put(1, 40.7, -74.0).unwrap();
        writer.put(3, 35.6, 139.6).unwrap();

        let reader = writer.finalize().unwrap();

        // Should still find all nodes via binary search
        assert!(reader.get(1).is_some());
        assert!(reader.get(3).is_some());
        assert!(reader.get(5).is_some());
        assert!(reader.get(2).is_none());
    }

    #[test]
    fn sparse_store_handles_large_node_ids() {
        let mut writer = NodeStoreWriter::new_sparse();
        let large_id = 13_000_000_000u64; // Typical max OSM node ID
        writer.put(large_id, 51.5, -0.1).unwrap();

        let reader = writer.finalize().unwrap();

        let (lon, lat) = reader.get(large_id).unwrap();
        assert!((lat - 51.5).abs() < 1e-7);
        assert!((lon - (-0.1)).abs() < 1e-7);
    }

    // ============================================
    // Dense (mmap) node store tests
    // ============================================

    #[test]
    fn dense_temp_store_put_and_get() {
        let mut writer = NodeStoreWriter::new_dense_temp(1000).unwrap();
        writer.put(1, 51.5, -0.1).unwrap();
        writer.put(2, 40.7, -74.0).unwrap();

        let reader = writer.finalize().unwrap();

        let (lon, lat) = reader.get(1).unwrap();
        assert!((lat - 51.5).abs() < 1e-7);
        assert!((lon - (-0.1)).abs() < 1e-7);

        let (lon, lat) = reader.get(2).unwrap();
        assert!((lat - 40.7).abs() < 1e-7);
        assert!((lon - (-74.0)).abs() < 1e-7);
    }

    #[test]
    fn dense_store_errors_on_node_id_exceeding_max() {
        let mut writer = NodeStoreWriter::new_dense_temp(100).unwrap();

        // Node ID within bounds should succeed
        assert!(writer.put(99, 51.5, -0.1).is_ok());

        // Node ID at boundary should fail
        let result = writer.put(100, 51.5, -0.1);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("exceeds"));
    }

    #[test]
    fn dense_store_returns_none_for_out_of_bounds() {
        let writer = NodeStoreWriter::new_dense_temp(100).unwrap();
        let reader = writer.finalize().unwrap();

        assert!(reader.get(100).is_none());
        assert!(reader.get(1000).is_none());
    }

    #[test]
    fn dense_store_returns_zero_for_unwritten_nodes() {
        let writer = NodeStoreWriter::new_dense_temp(100).unwrap();
        let reader = writer.finalize().unwrap();

        // Unwritten node returns (0.0, 0.0) - this is sparse file behavior
        let (lon, lat) = reader.get(50).unwrap();
        assert_eq!(lon, 0.0);
        assert_eq!(lat, 0.0);
    }

    // ============================================
    // Pack/unpack coords tests
    // ============================================

    #[test]
    fn pack_unpack_roundtrip() {
        let test_cases = vec![
            (51.5073509, -0.1277583),
            (40.7127753, -74.0059728),
            (35.6761919, 139.6503106),
            (-33.8688197, 151.2092955),
            (0.0, 0.0),
            (90.0, 180.0),
            (-90.0, -180.0),
        ];

        for (lat, lon) in test_cases {
            let packed = pack_coords(lat, lon);
            let (unpacked_lon, unpacked_lat) = unpack_coords(packed);

            assert!(
                (lat - unpacked_lat).abs() < 1e-7,
                "lat mismatch: {} vs {}",
                lat,
                unpacked_lat
            );
            assert!(
                (lon - unpacked_lon).abs() < 1e-7,
                "lon mismatch: {} vs {}",
                lon,
                unpacked_lon
            );
        }
    }

    // ============================================
    // All stores equivalence tests
    // ============================================

    #[test]
    fn all_stores_produce_same_results() {
        let test_coords = vec![
            (1u64, 51.5073509, -0.1277583),
            (2, 40.7127753, -74.0059728),
            (3, 35.6761919, 139.6503106),
            (4, -33.8688197, 151.2092955),
        ];

        // Memory store
        let mut mem_writer = NodeStoreWriter::new_memory();
        for (id, lat, lon) in &test_coords {
            mem_writer.put(*id, *lat, *lon).unwrap();
        }
        let mem_reader = mem_writer.finalize().unwrap();

        // Sparse store
        let mut sparse_writer = NodeStoreWriter::new_sparse();
        for (id, lat, lon) in &test_coords {
            sparse_writer.put(*id, *lat, *lon).unwrap();
        }
        let sparse_reader = sparse_writer.finalize().unwrap();

        // Dense store
        let mut dense_writer = NodeStoreWriter::new_dense_temp(1000).unwrap();
        for (id, lat, lon) in &test_coords {
            dense_writer.put(*id, *lat, *lon).unwrap();
        }
        let dense_reader = dense_writer.finalize().unwrap();

        // Compare results
        for (id, _, _) in &test_coords {
            let mem_result = mem_reader.get(*id).unwrap();
            let sparse_result = sparse_reader.get(*id).unwrap();
            let dense_result = dense_reader.get(*id).unwrap();

            assert!(
                (mem_result.0 - sparse_result.0).abs() < 1e-10,
                "lon mismatch between memory and sparse for node {id}"
            );
            assert!(
                (mem_result.1 - sparse_result.1).abs() < 1e-10,
                "lat mismatch between memory and sparse for node {id}"
            );
            assert!(
                (mem_result.0 - dense_result.0).abs() < 1e-10,
                "lon mismatch between memory and dense for node {id}"
            );
            assert!(
                (mem_result.1 - dense_result.1).abs() < 1e-10,
                "lat mismatch between memory and dense for node {id}"
            );
        }
    }
}
