use std::io::Result;
use std::net::TcpStream;

pub enum Package<'a> {
    OK,
    Error(&'a [u8]),
    Get(&'a [u8]),
    Set(&'a [u8], &'a [u8]),
    Remove(&'a [u8]),
}

impl<'a> std::fmt::Display for Package<'a>{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Package::Get(key) => writeln!(f, "package<get> {}", std::str::from_utf8(key).unwrap()),
            Package::Set(key, val) => writeln!(f, "package<set> {} {}", std::str::from_utf8(key).unwrap(), std::str::from_utf8(val).unwrap()),
            _ => unimplemented!(),
        }
    }
}

enum PackageType {
    OK,
    Error,
    Get,
    Set,
    Remove,
}

impl Into<PackageType> for u8 {
    fn into(self) -> PackageType {
        match self {
            0 => PackageType::OK,
            1 => PackageType::Error,
            2 => PackageType::Get,
            3 => PackageType::Set,
            4 => PackageType::Remove,
            _ => unimplemented!(),
        }
    }
}

// structure the package
// |type_of_message(1 byte)|double package(1 byte)|size_of_main_part(4 bytes)|body(unsized)|
pub fn construct_package(p: Package) -> Vec<u8> {
    let bsize = body_size(&p);
    let psize = package_size(&p) as usize;
    let mut buffer = vec![0; psize];

    match p {
        Package::Error(mss) => fill_single_buffer(&mut buffer, PackageType::Error, bsize, mss),
        Package::Get(key) => fill_single_buffer(&mut buffer, PackageType::Get, bsize, key),
        Package::Remove(key) => fill_single_buffer(&mut buffer, PackageType::Remove, bsize, key),
        Package::OK => fill_single_buffer(&mut buffer, PackageType::OK, 0, &[0]),
        Package::Set(key, val) => fill_double_buffer(&mut buffer, PackageType::Set, bsize, key, val),
    };

    buffer
}

pub fn deconstruct_package(b: &[u8]) -> Package {
    let default_part = prelude_size as usize;
    let b_size =  u32::from_be_bytes([b[2], b[3], b[4], b[5]]) as usize;
    let finish_body = default_part + b_size;
    println!("size {}s", b.len());
    match b[0].into() {
        PackageType::Get => Package::Get(&b[default_part..]),
        PackageType::Set => Package::Set(&b[default_part..finish_body], &b[finish_body ..]),
        _ => unimplemented!(),
    }
}

const prelude_size: u32 = 1 + 1 + 4;

pub fn package_size(p: &Package) -> u32 {
    prelude_size + body_size(p)
}

fn body_size(p: &Package) -> u32 {
    (match p {
        Package::Error(mss) => mss.len(),
        Package::Get(key) => key.len(),
        Package::Remove(key) => key.len(),
        Package::Set(key, val) => key.len() + val.len(),
        Package::OK => 0,
    }) as u32
}

fn fill_double_buffer(dst: &mut [u8], pt: PackageType, size: u32, src1: &[u8], src2: &[u8]) {
    let col = src1.iter().chain(src2).map(|e| *e).collect::<Vec<u8>>();
    fill_buffer(dst, pt, true, size - src2.len() as u32, &col);
}

fn fill_single_buffer(dst: &mut [u8], pt: PackageType, size: u32, src: &[u8]) {
    fill_buffer(dst, pt, false, size , src);
}

fn fill_buffer(dst: &mut [u8], pt: PackageType, is_double: bool, size: u32, src: &[u8]) {
    dst[0] = pt as u8;
    dst[1] = is_double as u8;
    fill_bytes(&mut dst[2..6], &size.to_be_bytes());
    fill_bytes(&mut dst[6..], src);
}

fn fill_bytes(dst: &mut [u8], src: &[u8]) {
    for i in 0..src.len() {
        dst[i] = src[i]
    }
}