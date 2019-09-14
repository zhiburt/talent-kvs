use std::collections::{HashMap, BTreeMap};
use std::fs::File;
use serde::{Deserialize, Serialize};
use std::path::{PathBuf, Path};
use std::ops::Range;
use std::io::{
    prelude::*,
    BufReader,
    BufWriter,
    SeekFrom,
    Seek,
};

static COMPACT_BOUND: u64 = 1001;

pub type Result<T> = std::result::Result<T, std::io::Error>;

#[derive(Serialize, Deserialize)]
enum Command {
    Remove { key: String },
    Set { key: String, val: String },
}

#[derive(Clone, Debug)]
struct CommandOp {
    pos: u64,
    len: u64,
    gen: Generation,
}

impl From<(Generation, Range<u64>)> for CommandOp {
    fn from((gen, range): (Generation, Range<u64>)) -> Self {
        CommandOp {
            pos: range.start,
            len: range.end - range.start,
            gen,
        }
    }
}

type Generation = u64;

/// KvStore represent simple key value storage
pub struct KvStore {
    index: HashMap<String, CommandOp>,
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
        for &gen in  &generations {
            readers.insert(gen, PositionBufReader::new(gen_file(gen, &path)?)?);
        }

        let current_generation = generations.last().map_or(0, |&g| g + 1);
        let (writer, reader) = new_generation_file(current_generation, &path)?;

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
        let element = self.index.get(&k);
        let command = match element {
            None => Err(std::io::Error::from(std::io::ErrorKind::InvalidData)),
            Some(op) => {
                let buffer = read_from_vec(self.readers.get_mut(&op.gen).expect("GG: cannot find"), op)?;
                deserialize(&buffer)
            }
        };

        eprintln!("inf {:?}", self.index);

        for &r in self.readers.keys() {
            eprintln!("-- inf {:?}\n", std::fs::read(log_path(r, &self.path))?);
        }

        match command {
            Ok(Command::Set { val, ..}) => Ok(Some(val)),
            _ => Ok(None)
        }
    }

    /// Set put new value in storage by key
    /// it rewrite value if that alredy exists
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        let command = Command::Set { key: key.clone(), val };
        let b = serialize(&command)?;
        let offset = write_to(&mut self.writer, &b)?;

        let command = CommandOp::from((self.generation, offset.start..offset.end));
        self.untracked += self.index.insert(key, command).map_or(0, |_| 1);

        if self.untracked > COMPACT_BOUND {
            self.compact()?;
        }

        Ok(())
    }                                

    /// Delete key value pair from storage
    pub fn remove(&mut self, key: String) -> Result<()> {
        if !self.index.contains_key(&key) {
            return Err(std::io::Error::from(std::io::ErrorKind::NotFound));
        }

        eprintln!("inf {:?}", self.index);

        self.untracked += self.index.remove(&key).map_or(0, |_| 1);
        self.write(&serialize(&Command::Remove{key})?)
    }

    fn compact(&mut self) -> Result<()> {
        let compact_gen = self.generation  + 1;
        let (cgw, cgr) = new_generation_file(compact_gen, &self.path)?;
        compact_to(&mut self.index, &mut self.readers, &mut PositionBufWriter::new(cgw)?, compact_gen)?;

        for (&gen, _) in &self.readers {
            std::fs::remove_file(log_path(gen, &self.path))?;
        }

        self.readers.clear();
        self.readers.insert(compact_gen, PositionBufReader::new(cgr)?);

        let current_gen = compact_gen + 1;
        let (cw, cr) =  new_generation_file(current_gen, &self.path)?;
        self.readers.insert(current_gen, PositionBufReader::new(cr)?);
        self.writer = PositionBufWriter::new(cw)?;

        self.untracked = 0;
        self.generation = current_gen;

        Ok(())
    }

    fn write(&mut self, b: &[u8]) -> Result<()> {
        write_to(&mut self.writer, b)?;
        Ok(())
    }
}

    fn write_to(mut writer: &mut PositionBufWriter<File>, b: &[u8]) -> Result<Range<u64>> {
        let start_position = writer.pos;
        writer.write(&b)?;
        writer.flush()?;

        Ok(start_position..writer.pos)
    }

    fn deserialize(bytes: &[u8]) -> Result<Command> {
        rmp_serde::decode::from_slice(&bytes).
            map_err(|_| { std::io::Error::from(std::io::ErrorKind::Interrupted) })
    }

    fn serialize(c: &Command) -> Result<(Vec<u8>)> {
        rmp_serde::encode::to_vec(&c).
            map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))
    }

    fn compact_to(
        index: &mut HashMap<String, CommandOp>,
        readers: &mut BTreeMap<Generation, PositionBufReader<File>>,
        writer: &mut PositionBufWriter<File>,
        gen: Generation) -> Result<()>
    {
        for pos in index.values_mut() {
            println!("{:?} {:?}", readers.keys(), pos);
            let reader = readers.get_mut(&pos.gen).expect("GG: cannot find");
            let content = read_from_vec(reader, pos)?;
            let offset = write_to(writer,&content)?;
            *pos = CommandOp::from((gen, offset.start..offset.end));
        }

        Ok(())
    }

    fn read_from_vec(reader: &mut PositionBufReader<File>, pos: &CommandOp) -> Result<Vec<u8>> {
        let mut buffer = Vec::with_capacity(pos.len as usize);
        buffer.resize(buffer.capacity(), 0);
        read_from(reader, &mut buffer, pos)?;

        Ok(buffer)
    }

    fn read_from(reader: &mut PositionBufReader<File>, buf: &mut [u8], pos: &CommandOp) -> Result<()> {
        reader.seek(SeekFrom::Start(pos.pos))?;
        let mut content = reader.take(pos.len);
        content.read(buf)?;

        Ok(())
    }

    fn upload_index(index: &mut HashMap<String, CommandOp>, mut reader: &mut PositionBufReader<File>, gen: Generation) -> Result<u64> {
        let mut start = 0u64;
        let mut untracked = 0;
        while let Ok(command) =  rmp_serde::decode::from_read(&mut reader) {
                let overwritten = match command {
                    Command::Set { key, .. } => index.insert(key, CommandOp::from((gen, start..reader.pos))),
                    Command::Remove { key } => index.remove(&key),
                };
                untracked += overwritten.map_or(0, |_| 1);

                start = reader.pos;
        }

        Ok(untracked)
    }

    fn state(path: &PathBuf) -> Result<Vec<Generation>> {
        let mut generations: Vec<u64> = std::fs::read_dir(path)?.
            flat_map(|entry| -> Result<_> { Ok(entry?.path()) }).
            filter(|path| path.is_file() && path.extension() == Some("sil".as_ref())).
            flat_map(|fname| {
                fname.file_stem().
                    and_then(std::ffi::OsStr::to_str).
                    map(str::parse::<Generation>)
            }).
            flatten().
            collect();

        generations.sort_unstable();

        Ok(generations)
    }

    fn gen_file(gen: Generation, path: &PathBuf) -> Result<File> {
        gen_file_ops(gen, path, None)
    }

    fn new_generation_file(gen: Generation, path: &PathBuf) -> Result<(File, File)> {
        let mut ops = std::fs::OpenOptions::new();
        ops.read(true).
            write(true).
            create(true).
            append(true);

        let writer = gen_file_ops(gen, path, Some(ops))?;
        let reader = gen_file_ops(gen, path, None)?;

        Ok((writer, reader))
    }

    fn gen_file_ops(gen: Generation, path: &PathBuf, options: Option<std::fs::OpenOptions>) -> Result<File> {
        let p = log_path(gen, path);
        match options {
            Some(ops) => ops.open(p),
            None => File::open(p),
        }
    }

    fn log_path(gen: u64, dir: &Path) -> PathBuf {
        dir.join(format!("{}.sil", gen))
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
