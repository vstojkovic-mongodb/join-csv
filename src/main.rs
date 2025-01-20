use std::collections::HashMap;
use std::fs::File;
use std::io::{stdin, Read};
use std::path::{Path, PathBuf};

use anyhow::Result;
use csv::{Reader, Writer};
use indexmap::IndexMap;
use serde::Deserialize;
use serde_yaml::from_reader;

#[derive(Debug, Deserialize)]
struct JoinSpec {
    key: Vec<String>,
    sources: Vec<Source>,
    output: PathBuf,
}

#[derive(Debug, Deserialize)]
struct Source {
    path: PathBuf,
    projections: IndexMap<String, String>,
}

type Key = Vec<String>;
type Projection = Vec<String>;
type Data = IndexMap<Key, Projection>;
type JoinInput = Vec<Data>;

fn read_file<'k, 'p>(
    path: &Path,
    key_spec: impl Iterator<Item = &'k str>,
    proj_spec: impl Iterator<Item = &'p str>,
) -> Result<Data> {
    let mut reader = Reader::from_path(path)?;

    let headers: HashMap<_, _> = reader
        .headers()?
        .into_iter()
        .enumerate()
        .map(|(idx, col)| (col.to_owned(), idx))
        .collect();
    let key_idx: Vec<_> = key_spec
        .into_iter()
        .map(|col| *headers.get(col).unwrap())
        .collect();
    let proj_idx: Vec<_> = proj_spec
        .into_iter()
        .map(|col| *headers.get(col).unwrap())
        .collect();

    let mut data = IndexMap::new();
    for record in reader.into_records() {
        let record = record?;
        let key: Key = key_idx
            .iter()
            .map(|&idx| record.get(idx).unwrap().to_owned())
            .collect();
        let projection: Projection = proj_idx
            .iter()
            .map(|&idx| record.get(idx).unwrap().to_owned())
            .collect();
        data.insert(key, projection);
    }

    Ok(data)
}

fn read_input(spec: &JoinSpec) -> Result<JoinInput> {
    let mut join_input = Vec::with_capacity(spec.sources.len());
    for source in spec.sources.iter() {
        let data = read_file(
            &source.path,
            spec.key.iter().map(String::as_str),
            source.projections.keys().into_iter().map(String::as_str),
        )?;
        join_input.push(data);
    }
    Ok(join_input)
}

fn write_output(spec: &JoinSpec, input: JoinInput) -> Result<()> {
    let num_cols = spec.key.len()
        + spec
            .sources
            .iter()
            .map(|source| source.projections.len())
            .sum::<usize>();
    let mut writer = Writer::from_path(&spec.output)?;

    let mut row = Vec::with_capacity(num_cols);
    for col in spec.key.iter() {
        row.push(col.clone());
    }
    for source in spec.sources.iter() {
        for col in source.projections.values() {
            row.push(col.clone());
        }
    }
    writer.write_record(&row)?;

    for (key, projection) in input[0].iter() {
        row.clear();
        row.extend_from_slice(key);
        row.extend_from_slice(projection);
        for source_data in &input[1..] {
            row.extend_from_slice(source_data.get(key).unwrap());
        }
        writer.write_record(&row)?;
    }

    Ok(())
}

fn load_spec(reader: impl Read) -> Result<JoinSpec> {
    Ok(from_reader(reader)?)
}

fn main() -> Result<()> {
    let spec = match std::env::args_os().nth(1) {
        Some(path) => load_spec(File::open(path)?)?,
        None => load_spec(stdin())?,
    };
    let input = read_input(&spec)?;
    write_output(&spec, input)?;
    Ok(())
}
