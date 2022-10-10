use std::fs::{create_dir_all, metadata, File};
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

pub fn tmp_file(dir: &Path, file_name: &str, contents: &str) -> PathBuf {
    create_dir_all(dir).unwrap();
    let path = dir.join(file_name);
    let mut file = File::create(&path).unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    return path;
}

pub fn same(file1: &Path, file2: &Path) -> bool {
    let metadata1 = metadata(file1).unwrap();
    let metadata2 = metadata(file2).unwrap();
    metadata1.ino() == metadata2.ino()
}
