use criterion::Criterion;
use criterion::black_box;
use criterion::{criterion_group, criterion_main};
use rand_pcg::rand_core::RngCore;
use rand_pcg::rand_core::SeedableRng;
use rand::{Rng};
use kvs::{KvsEngine, KvStore, SledStorage};

fn read_kvs_benchmark(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let mut kvs = KvStore::open(dir.path()).unwrap();

    let mut data = Vec::new();
    for i in 0..1000 {
        let key = random_data(10, 3);
        let val = random_data(10, 3);
        kvs.set(key.clone(), key.clone()).unwrap();
        data.push((key, val));
    }

    c.bench_function("read", |b| {
        let (key, val) = data.pop().unwrap();
        b.iter(|| {
            assert_eq!(kvs.get(key.clone()).unwrap(), Some(val.clone()));
    })});
}

fn read_sled_benchmark(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let mut kvs = SledStorage::open(dir.path()).unwrap();

    let mut data = Vec::new();
    for _ in 0..1000 {
        let key = random_data(10, 3);
        let val = random_data(10, 3);
        kvs.set(key.clone(), key.clone()).unwrap();
        data.push((key, val));
    }

    c.bench_function("read", |b| {
        for (key, val) in &data {
            b.iter(|| {
                assert_eq!(kvs.get(key.clone()).unwrap(), Some(val.clone()));
            })
        };
     });
}

fn write_kvs_benchmark(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let mut kvs = KvStore::open(dir.path()).unwrap();
    
    c.bench_function("write", |b| {
        let key = random_data(10, 100_000);
        let data = random_data(10, 100_000);
        b.iter(|| {
            assert!(kvs.set(key.clone(), data.clone()).is_ok());
    })});
}

fn write_sled_benchmark(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let mut kvs = SledStorage::open(dir.path()).unwrap();
    
    c.bench_function("write", |b| {
        let key = random_data(10, 100_000);
        let data = random_data(10, 100_000);
        b.iter(|| {
            assert!(kvs.set(key.clone(), data.clone()).is_ok());
    })});
}

criterion_group!(benches, write_kvs_benchmark, write_sled_benchmark, read_kvs_benchmark, read_sled_benchmark);
criterion_main!(benches);

fn random_data(iv: u64, len: usize) -> String {
    let mut data = vec![0; len];
    let mut rng = rand_pcg::Pcg32::seed_from_u64(iv);
    let mut i = 0;
    while i < len {
        data[i] = rng.gen_range(0, 127);
        i += 1;
    }

    String::from_utf8(data).unwrap()
}
