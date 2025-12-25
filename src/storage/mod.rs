use anyhow::{Context, Result, anyhow};
use memmap2::{Mmap, MmapMut};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::path::Path;

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
    Mmap(MmapNodeStoreWriter),
    Memory(MemoryNodeStore),
}

enum NodeStoreReaderImpl {
    Mmap(MmapNodeStoreReader),
    Memory(MemoryNodeStore),
}

struct MmapNodeStoreWriter {
    mmap: MmapMut,
    max_nodes: u64,
}

struct MmapNodeStoreReader {
    mmap: Mmap,
    max_nodes: u64,
}

#[derive(Clone)]
struct MemoryNodeStore {
    nodes: HashMap<u64, (i32, i32)>,
}

impl NodeStoreWriter {
    pub fn new_mmap<P: AsRef<Path>>(path: P, max_nodes: u64) -> Result<Self> {
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
            inner: NodeStoreWriterImpl::Mmap(MmapNodeStoreWriter { mmap, max_nodes }),
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
            NodeStoreWriterImpl::Mmap(store) => store.put(id, lat, lon),
            NodeStoreWriterImpl::Memory(store) => store.put(id, lat, lon),
        }
    }

    pub fn finalize(self) -> Result<NodeStoreReader> {
        match self.inner {
            NodeStoreWriterImpl::Mmap(store) => store.finalize(),
            NodeStoreWriterImpl::Memory(store) => Ok(NodeStoreReader {
                inner: NodeStoreReaderImpl::Memory(store),
            }),
        }
    }
}

impl NodeStoreReader {
    pub fn get(&self, id: u64) -> Option<(f64, f64)> {
        match &self.inner {
            NodeStoreReaderImpl::Mmap(store) => store.get(id),
            NodeStoreReaderImpl::Memory(store) => store.get(id),
        }
    }
}

impl MmapNodeStoreWriter {
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
            inner: NodeStoreReaderImpl::Mmap(MmapNodeStoreReader {
                mmap,
                max_nodes: self.max_nodes,
            }),
        })
    }
}

impl MmapNodeStoreReader {
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
