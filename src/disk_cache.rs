use anyhow::Result;
use futures::future::join_all;
use sccache::lru_disk_cache::Meter;
use std::{collections::hash_map::RandomState, hash::BuildHasher, path::PathBuf, sync::Arc};

use crate::FileNotifier;

pub struct DiskCacheMeter {}

impl<K> Meter<K, u64> for DiskCacheMeter {
    type Measure = usize;

    fn measure<Q: ?Sized>(&self, _: &Q, value: &u64) -> Self::Measure
    where
        K: std::borrow::Borrow<Q>,
    {
        *value as usize
    }
}

pub struct DiskCache<N, H: BuildHasher = RandomState> {
    root: PathBuf,
    lru: sccache::lru_disk_cache::LruCache<String, u64, H, DiskCacheMeter>,
    files_to_evict: u64,
    notifier: N,
}

impl<N: FileNotifier> DiskCache<N> {
    pub fn new<P: Into<PathBuf>>(root: P, files_to_evict: u64, capacity: u64, notifier: N) -> Self {
        let root = root.into();
        let meter = DiskCacheMeter {};
        let lru = sccache::lru_disk_cache::LruCache::with_meter(capacity, meter);
        std::fs::create_dir_all(&root).unwrap();
        Self {
            root,
            lru,
            files_to_evict,
            notifier,
        }
    }

    pub fn touch<S: AsRef<str>>(&mut self, key: S) {
        self.lru.get(key.as_ref());
    }

    pub async fn get<S: AsRef<str>>(&mut self, key: S) -> Result<Option<tokio::fs::File>> {
        self.lru.get(key.as_ref());

        let path = self.root.join(key.as_ref());
        match tokio::fs::File::open(path).await {
            Ok(file) => Ok(Some(file)),
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    Ok(None)
                } else {
                    Err(e.into())
                }
            }
        }
    }

    pub async fn insert<S: AsRef<str>>(&mut self, key: S, buf: &[u8]) -> Result<()> {
        if self.lru.contains_key(key.as_ref()) {
            return Ok(());
        }

        if self.lru.size() + buf.len() as u64 > self.lru.capacity() {
            self.evict().await?;
        }
        let path = self.root.join(key.as_ref());
        self.lru.insert(key.as_ref().to_owned(), buf.len() as u64);
        tokio::fs::write(path, buf).await?;
        self.notifier.added(key.as_ref().to_owned()).await;
        Ok(())
    }

    async fn evict(&mut self) -> Result<()> {
        let files_to_evict = (1..self.files_to_evict)
            .filter_map(|_| self.lru.remove_lru())
            .map(|(key, size)| {
                let path = self.root.join(&key);
                (key, size, path)
            })
            .collect::<Vec<_>>();

        let lru = &Arc::new(tokio::sync::Mutex::new(&mut self.lru));
        let notifier = &self.notifier;
        join_all(
            files_to_evict
                .into_iter()
                .map(|(key, size, path)| async move {
                    if let Err(_) = tokio::fs::remove_file(path).await {
                        lru.lock().await.insert(key, size);
                    } else {
                        notifier.removed(key).await;
                    }
                }),
        )
        .await;

        Ok(())
    }
}
