use std::collections::{HashMap, BTreeMap};
use std::fs::File;
use serde::{Deserialize, Serialize};
use std::io::{
    prelude::*,
    BufReader,
    BufWriter,
    SeekFrom,
    Seek,
};

pub type Result<T> = std::result::Result<T, std::io::Error>;

#[derive(Serialize, Deserialize)]
enum Command {
    Remove { key: String },
    Set { key: String, val: String },
}

#[derive(Clone, Debug)]
struct CommandOp {
    start: u64,
    end: u64
}

impl<T:  std::ops::RangeBounds<u64>> From<T> for CommandOp {
    fn from(range: T) -> Self {
        let start = match range.start_bound() {
            std::ops::Bound::Included(val) => *val,
            _ => 0
        };
        let end = match range.end_bound() {
            std::ops::Bound::Excluded(val) => *val,
            _ => 0
        };

        CommandOp {
            start: start,
            end: end,
        }
    }
}

/// KvStore represent simple key value storage
pub struct KvStore {
    store: HashMap<String, CommandOp>,
    storage_w: PositionBufWriter<File>,
    storage_r: PositionBufReader<File>,
    path: std::path::PathBuf,
    untracked: u64,
}

impl KvStore {
    /// Create new object of storage
    pub fn open(folder: &std::path::Path) -> Result<Self> {
        let path = folder.join("log.zs");
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&path)?;
        let f1 = std::fs::File::open(std::path::Path::new(&path))?;

        let mut kv = KvStore {
            store: HashMap::new(),
            storage_w: PositionBufWriter::new(f)?,
            storage_r: PositionBufReader::new(f1)?,
            path: path,
            untracked: 0,
        };

        kv.init()?;

        Ok(kv)
    }

    /// Get method tries to find value with `key`
    pub fn get(&mut self, k: String) -> Result<Option<String>> {
        let index = self.store.get(&k);
        
        let command = match index {
            None => Err(std::io::Error::from(std::io::ErrorKind::InvalidData)),
            Some(op) => {
                let mut buffer = Vec::with_capacity((op.end - op.start) as usize);
                buffer.resize(buffer.capacity(), 0);
                self.read_from(&mut buffer, std::io::SeekFrom::Start(op.start))?;
                self.deserialize(&buffer)
            }
        };

        match command {
            Ok(Command::Set { val, ..}) => Ok(Some(val)),
            _ => Ok(None)
        }
    }

    /// Set put new value in storage by key
    /// it rewrite value if that alredy exists
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        let c = Command::Set { key: key.clone(), val };
        let position = self.storage_w.pos;
        let b = self.serialize(&c)?;
        self.write_to_file(&b)?;

        self.untracked += self.store.insert(key, CommandOp::from(position.. self.storage_w.pos)).map_or(0, |_| 1);
        if self.untracked > 40 {
            self.compact()?;
        }

        Ok(())
    }                                

    /// Delete key value pair from storage
    pub fn remove(&mut self, key: String) -> Result<()> {
        if !self.store.contains_key(&key) {
            return Err(std::io::Error::from(std::io::ErrorKind::NotFound));
        }

        self.untracked += self.store.remove(&key).map_or(0, |_| 1);
        self.write_to_file(
            &self.serialize(&Command::Remove{key})?
        )
    }

    fn init(&mut self) -> Result<()> {
        let mut start = 0u64;
        while let Ok(command) =  rmp_serde::decode::from_read(&mut self.storage_r) {
                let overwritten = match command {
                    Command::Set { key, .. } => self.store.insert(key, CommandOp::from(start..self.storage_r.pos)),
                    Command::Remove { key } => self.store.remove(&key),
                };
                self.untracked += overwritten.map_or(0, |_| 1);

                start = self.storage_r.pos;
        }

        self.storage_w.seek(std::io::SeekFrom::Start(self.storage_r.pos))?;

        Ok(())
    }

    fn write_to_file(&mut self, b: &[u8]) -> Result<()> {
        self.storage_w.write(&b)?;
        self.storage_w.flush()
    }

    fn read_from(&mut self, buf: &mut [u8], offset: std::io::SeekFrom) -> Result<()> {
        self.storage_r.seek(offset)?;
        self.storage_r.read(buf)?;
        Ok(())
    }

    fn deserialize(&self, bytes: &[u8]) -> Result<Command> {
        rmp_serde::decode::from_slice(&bytes).
            map_err(|_| { std::io::Error::from(std::io::ErrorKind::Interrupted) })
    }

    fn serialize(&self, c: &Command) -> Result<(Vec<u8>)> {
        rmp_serde::encode::to_vec(&c).
            map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))
    }

    fn compact(&mut self) -> Result<()> {
        let mut bin_commands = Vec::new();
        let store = self.store.iter().map(|op| (op.0.clone(), op.1.clone())).collect::<Vec<(String, CommandOp)>>();

        for (key, offset) in store.iter() {
            let mut buffer = Vec::with_capacity((offset.end - offset.start) as usize);
            buffer.resize(buffer.capacity(), 0);
            self.read_from(&mut buffer, std::io::SeekFrom::Start(offset.start))?;

            match self.deserialize(&buffer)? {
                Command::Set{..} => {  bin_commands.push((key, buffer)); },
                _ => ()
            }
        }

        self.storage_w.writer.get_mut().set_len(0)?;
        self.storage_w.seek(std::io::SeekFrom::Start(0))?;
        self.storage_r.seek(std::io::SeekFrom::Start(0))?;
        let mut start = 0u64;
        for (key, bin) in bin_commands {
            self.write_to_file(&bin)?;
            let mut k = self.store.get_mut(key).unwrap();
            k.start = start;
            k.end = self.storage_w.pos;
            start = self.storage_w.pos;
        }

        self.untracked  = 0;

        Ok(())
    }
}

struct PositionBufReader<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> PositionBufReader<R> {
    fn new(mut reader: R) -> Result<Self> {
        let current_pos = reader.seek(SeekFrom::Current(0))?;

        Ok(PositionBufReader {
            reader: BufReader::new(reader),
            pos: current_pos,
        })
    }
}

impl<R: Read + Seek> Seek for PositionBufReader<R> {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.pos = self.reader.seek(pos)?;
        
        Ok(self.pos)
    }
}

impl<R: Read + Seek> Read for PositionBufReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let size = self.reader.read (buf)?;
        self.pos += size as u64;

        Ok(size)
    }
}

struct PositionBufWriter<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> PositionBufWriter<W> {
    fn new(mut writer: W) -> Result<Self> {
        let current_pos = writer.seek(SeekFrom::Current(0))?;

        Ok(PositionBufWriter {
            writer: BufWriter::new(writer),
            pos: current_pos,
        })
    }
}

impl<W: Write + Seek> Seek for PositionBufWriter<W> {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.pos = self.writer.seek(pos)?;
        
        Ok(self.pos)
    }
}

impl<W: Write + Seek> Write for PositionBufWriter<W> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let size = self.writer.write(buf)?;
        self.pos += size as u64;

        Ok(size)
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush()
    }
}

impl Drop for KvStore  { 
    fn drop(&mut self){
        self.storage_w.flush();
    }
}
