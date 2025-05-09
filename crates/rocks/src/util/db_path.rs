use {
    rocksdb::{Options, DB},
    std::path::{Path, PathBuf},
};

/// Temporary database path which calls DB::Destroy when DBPath is dropped.
/// See https://github.com/rust-rocksdb/rust-rocksdb/blob/master/tests/util/mod.rs for details.
#[allow(dead_code)]
pub struct DBPath {
    dir: tempfile::TempDir, // kept for cleaning up during drop
    path: PathBuf,
}

impl DBPath {
    /// Produces a fresh (non-existent) temporary path which will be
    /// DB::destroy'ed automatically.
    pub fn new(prefix: &str) -> DBPath {
        let dir = tempfile::Builder::new()
            .prefix(prefix)
            .tempdir()
            .expect("Failed to create temporary path for db.");
        let path = dir.path().join("db");

        DBPath { dir, path }
    }
}

impl Drop for DBPath {
    fn drop(&mut self) {
        let opts = Options::default();
        tracing::debug!("Destroying temporary DB at {:?}", self.path);
        DB::destroy(&opts, &self.path).expect("Failed to destroy temporary DB");
    }
}

/// Convert a DBPath ref to a Path ref.
/// We don't implement this for DBPath values because we want them to
/// exist until the end of their scope, not get passed in to functions and
/// dropped early.
impl AsRef<Path> for &DBPath {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}
