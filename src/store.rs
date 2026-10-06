use crate::policy::State;
use anyhow::{Context, Result, ensure};
use fs2::FileExt;
use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

pub struct Store {
    dir: PathBuf,
    _lock: File,
}

impl Store {
    pub fn open(dir: &Path) -> Result<(Self, State)> {
        std::fs::create_dir_all(dir)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.join("lock"))?;
        lock.try_lock_exclusive()
            .context("another worker holds the state lock")?;
        let state = match std::fs::read(dir.join("state.json")) {
            Ok(bytes) => serde_json::from_slice::<State>(&bytes)
                .context("invalid state; refusing to reset safety limits")?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => State::fresh(),
            Err(e) => return Err(e.into()),
        };
        ensure!(state.version == 1, "unsupported state version");
        Ok((
            Self {
                dir: dir.to_owned(),
                _lock: lock,
            },
            state,
        ))
    }

    pub fn save(&self, state: &State) -> Result<()> {
        let mut f = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(self.dir.join("state.tmp"))?;
        f.write_all(&serde_json::to_vec(state)?)?;
        f.sync_all()?;
        std::fs::rename(self.dir.join("state.tmp"), self.dir.join("state.json"))?;
        File::open(&self.dir)?.sync_all()?;
        Ok(())
    }
}
