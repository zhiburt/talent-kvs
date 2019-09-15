
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{prelude::*, BufReader, BufWriter, Seek, SeekFrom};
use std::ops::Range;
use std::path::{Path, PathBuf};
use crate::{KvsError, Result};

static COMPACT_BOUND: u64 = 1001;

type Generation = u64;

/// KvStore represent simple key value storage
pub struct KvStore {
    index: HashMap<String, CommandPos>,
    readers: BTreeMap<Generation, PositionBufReader<File>>,
    writer: PositionBufWriter<File>,
    path: std::path::PathBuf,
    untracked: u64,
    generation: Generation,
}

impl KvStore {
    /// Create new object of storage
    pub fn open(folder: impl Into<PathBuf>) -> Result<Self> {
        let path = folder.into();
        let mut readers = BTreeMap::new();

        let generations = state(&path)?;
        for &gen in &generations {
            readers.insert(gen, PositionBufReader::new(gen_file(gen, &path)?)?);
        }

        let current_generation = generations.last().map_or(0, |&g| g + 1);
        let (writer, reader) = create_generation_files(current_generation, &path)?;

        readers.insert(current_generation, PositionBufReader::new(reader)?);

        let mut index = HashMap::new();
        let mut untracked = 0;
        for (&gen, reader) in &mut readers {
            untracked += upload_index(&mut index, reader, gen)?
        }

        Ok(KvStore {
            index: index,
            writer: PositionBufWriter::new(writer)?,
            readers: readers,
            path: path,
            untracked: untracked,
            generation: current_generation,
        })
    }

    /// Get method tries to find value with `key`
    pub fn get(&mut self, k: String) -> Result<Option<String>> {
        match self.index.get(&k) {
            None => Ok(None),
            Some(op) => {
                let b = read_to_vec(self.readers.get_mut(&op.gen).expect("GG: cannot find"), op)?;
                match deserialize(&b) {
                    Ok(Command::Set { val, .. }) => Ok(Some(val)),
                    _ => Err(KvsError::AppropriateCommandNotFound),
                }
            }
        }
    }

    /// Set put new value in storage by key
    /// it rewrite value if that alredy exists
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        let command = Command::Set {
            key: key.clone(),
            val,
        };
        let b = serialize(&command)?;
        let offset = write_to(&mut self.writer, &b)?;

        let command = CommandPos::from((self.generation, offset.start..offset.end));
        self.untracked += self.index.insert(key, command).map_or(0, |old| old.len);

        if self.untracked > COMPACT_BOUND {
            self.compact()?;
        }

        Ok(())
    }

    /// Delete key value pair from storage
    pub fn remove(&mut self, key: String) -> Result<()> {
        if !self.index.contains_key(&key) {
            return Err(KvsError::KeyNotFound);
        }

        self.untracked += self.index.remove(&key).map_or(0, |old| old.len);
        self.write(&serialize(&Command::Remove { key })?)
    }

    fn compact(&mut self) -> Result<()> {
        let compact_gen = self.generation + 1;
        self.generation = self.generation + 2;
        let (mut compact_w, compact_r) = create_buf_generation_files(compact_gen, &self.path)?;
        compact_to(
            &mut self.index,
            &mut self.readers,
            &mut compact_w,
            compact_gen,
        )?;

        for (&gen, _) in &self.readers {
            std::fs::remove_file(gen_path(gen, &self.path))?;
        }

        self.readers.clear();
        self.readers.insert(compact_gen, compact_r);

        let (cw, cr) = create_buf_generation_files(self.generation, &self.path)?;
        self.readers.insert(self.generation, cr);
        self.writer = cw;
        self.untracked = 0;

        Ok(())
    }

    fn write(&mut self, b: &[u8]) -> Result<()> {
        write_to(&mut self.writer, b)?;
        Ok(())
    }
}

fn deserialize(bytes: &[u8]) -> Result<Command> {
    Ok(rmp_serde::decode::from_slice(&bytes)?)
}

fn serialize(c: &Command) -> Result<(Vec<u8>)> {
    Ok(rmp_serde::encode::to_vec(&c)?)
}

fn compact_to(
    index: &mut HashMap<String, CommandPos>,
    readers: &mut BTreeMap<Generation, PositionBufReader<File>>,
    writer: &mut PositionBufWriter<File>,
    gen: Generation,
) -> Result<()> {
    for pos in index.values_mut() {
        let reader = readers.get_mut(&pos.gen).expect("GG: cannot find");
        let content = read_to_vec(reader, pos)?;
        let offset = write_to(writer, &content)?;
        *pos = CommandPos::from((gen, offset.start..offset.end));
    }

    Ok(())
}

fn write_to(writer: &mut PositionBufWriter<File>, b: &[u8]) -> Result<Range<u64>> {
    let start_position = writer.pos;
    writer.write(&b)?;
    writer.flush()?;
    Ok(start_position..writer.pos)
}

fn read_to_vec(reader: &mut PositionBufReader<File>, pos: &CommandPos) -> Result<Vec<u8>> {
    let mut buffer = Vec::with_capacity(pos.len as usize);
    buffer.resize(buffer.capacity(), 0);
    read_from(reader, &mut buffer, pos)?;

    Ok(buffer)
}

fn read_from(reader: &mut PositionBufReader<File>, buf: &mut [u8], pos: &CommandPos) -> Result<()> {
    reader.seek(SeekFrom::Start(pos.pos))?;
    let mut content = reader.take(pos.len);
    content.read(buf)?;

    Ok(())
}

fn upload_index(
    index: &mut HashMap<String, CommandPos>,
    mut reader: &mut PositionBufReader<File>,
    gen: Generation,
) -> Result<u64> {
    let mut start = 0u64;
    let mut untracked = 0;
    while let Ok(command) = rmp_serde::decode::from_read(&mut reader) {
        let cleaned_bytes = match command {
            Command::Set { key, .. } => index
                .insert(key, CommandPos::from((gen, start..reader.pos)))
                .map_or(0, |old| old.len),
            Command::Remove { key } => {
                let old_bytes = index.remove(&key).map_or(0, |old| old.len);
                let this_command_bytes = reader.pos - start;
                old_bytes + this_command_bytes
            }
        };
        untracked += cleaned_bytes;
        start = reader.pos;
    }

    Ok(untracked)
}

fn state(path: &PathBuf) -> Result<Vec<Generation>> {
    let mut generations: Vec<u64> = std::fs::read_dir(path)?
        .flat_map(|entry| -> Result<_> { Ok(entry?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("sil".as_ref()))
        .flat_map(|fname| {
            fname
                .file_stem()
                .and_then(std::ffi::OsStr::to_str)
                .map(str::parse::<Generation>)
        })
        .flatten()
        .collect();

    generations.sort_unstable();

    Ok(generations)
}

fn gen_file(gen: Generation, path: &PathBuf) -> std::io::Result<File> {
    gen_file_ops(gen, path, None)
}

fn create_generation_files(gen: Generation, path: &PathBuf) -> Result<(File, File)> {
    let mut ops = std::fs::OpenOptions::new();
    ops.read(true).write(true).create(true).append(true);

    let writer = gen_file_ops(gen, path, Some(ops))?;
    let reader = gen_file_ops(gen, path, None)?;

    Ok((writer, reader))
}

fn gen_file_ops(
    gen: Generation,
    path: &PathBuf,
    options: Option<std::fs::OpenOptions>,
) -> std::io::Result<File> {
    let p = gen_path(gen, path);
    match options {
        Some(ops) => ops.open(p),
        None => File::open(p),
    }
}

fn create_buf_generation_files(
    gen: Generation,
    path: &PathBuf,
) -> Result<(PositionBufWriter<File>, PositionBufReader<File>)> {
    let (writer, reader) = create_generation_files(gen, path)?;

    Ok((
        PositionBufWriter::new(writer)?,
        PositionBufReader::new(reader)?,
    ))
}

fn gen_path(gen: u64, dir: &Path) -> PathBuf {
    dir.join(format!("{}.sil", gen))
}

#[derive(Serialize, Deserialize)]
enum Command {
    Remove { key: String },
    Set { key: String, val: String },
}

#[derive(Clone, Debug)]
struct CommandPos {
    pos: u64,
    len: u64,
    gen: Generation,
}

impl From<(Generation, Range<u64>)> for CommandPos {
    fn from((gen, range): (Generation, Range<u64>)) -> Self {
        CommandPos {
            pos: range.start,
            len: range.end - range.start,
            gen,
        }
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
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.pos = self.reader.seek(pos)?;

        Ok(self.pos)
    }
}

impl<R: Read + Seek> Read for PositionBufReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let size = self.reader.read(buf)?;
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
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.pos = self.writer.seek(pos)?;

        Ok(self.pos)
    }
}

impl<W: Write + Seek> Write for PositionBufWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let size = self.writer.write(buf)?;
        self.pos += size as u64;

        Ok(size)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}
