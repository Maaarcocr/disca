use async_trait::async_trait;

mod disk_cache;
mod file_sharing;

use anyhow::Result;
pub use disk_cache::DiskCache;
pub use file_sharing::FileSharingP2P;
use libp2p::Multiaddr;
use tokio::fs::File;

#[async_trait]
pub trait FileProvider {
    fn get_file(&mut self, path: String) -> Option<Vec<u8>>;
}

#[async_trait]
pub trait FileNotifier {
    async fn added(&self, path: String);
    async fn removed(&self, path: String);
}

#[async_trait]
impl FileNotifier for FileSharingP2P {
    async fn added(&self, path: String) {
        self.add_file(path).await.unwrap();
    }

    async fn removed(&self, path: String) {
        self.remove_file(path).await.unwrap();
    }
}

pub struct Disca {
    file_sharing: FileSharingP2P,
    disk_cache: DiskCache<FileSharingP2P>,
}

pub struct DiscaFileProvider {
    root: std::path::PathBuf,
}

impl FileProvider for DiscaFileProvider {
    fn get_file(&mut self, path: String) -> Option<Vec<u8>> {
        let path = self.root.join(path);
        std::fs::read(path).ok()
    }
}

impl Disca {
    pub async fn new<P: Into<std::path::PathBuf>>(
        root: P,
        files_to_evict: u64,
        capacity: u64,
        addr: Multiaddr,
    ) -> Result<Self> {
        let root = root.into();
        let file_sharing =
            FileSharingP2P::new(addr, DiscaFileProvider { root: root.clone() }).await?;

        let disk_cache = DiskCache::new(root, files_to_evict, capacity, file_sharing.clone());
        Ok(Self {
            file_sharing,
            disk_cache,
        })
    }

    pub async fn get(&mut self, path: String) -> Result<Option<File>> {
        let file = self.disk_cache.get(&path).await?;
        if let Some(file) = file {
            return Ok(Some(file));
        } else {
            let file_content = self.file_sharing.get_file(path.clone()).await?;
            if let Some(file_content) = file_content {
                self.disk_cache.insert(&path, &file_content).await?;
                let file = self.disk_cache.get(&path).await?;
                return Ok(file);
            }
        }
        Ok(None)
    }

    pub async fn add(&mut self, key: &str, content: &[u8]) -> Result<()> {
        self.disk_cache.insert(key, content).await?;
        Ok(())
    }

    pub async fn add_peer(&mut self, addr: Multiaddr) -> Result<()> {
        self.file_sharing.add_peer(addr).await?;
        Ok(())
    }

    pub fn addr(&self) -> &Multiaddr {
        self.file_sharing.addr()
    }

    pub fn peer_id(&self) -> &libp2p::PeerId {
        self.file_sharing.peer_id()
    }
}
