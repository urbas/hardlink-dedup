use log::warn;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs::{hard_link, metadata, remove_file, rename, File};
use std::io;
use std::io::{BufReader, Read, Result};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use uuid::Uuid;
use walkdir::{DirEntry, DirEntryExt, WalkDir};

struct Progress {
    total: usize,
    processed: usize,
    bytes_deduped: usize,
}

impl Progress {
    fn new(total: usize) -> Progress {
        Progress {
            total: total,
            processed: 0,
            bytes_deduped: 0,
        }
    }
}

impl std::fmt::Display for Progress {
    fn fmt(
        &self,
        formatter: &mut std::fmt::Formatter<'_>,
    ) -> std::result::Result<(), std::fmt::Error> {
        let percentage = if self.total == 0 {
            100.0
        } else {
            (self.processed as f64) / (self.total as f64) * 100.0
        };
        write!(
            formatter,
            "{:.2}%; {} bytes deduped",
            percentage, self.bytes_deduped
        )
    }
}

pub fn dedup(paths: &Vec<PathBuf>, dry_run: bool, paranoid: bool) {
    let inode_to_paths = find_inode_groups(paths);
    let files = inode_to_paths
        .values()
        .map(|file_group| file_group.iter().nth(0))
        .flatten();
    println!("Processing {} files.", inode_to_paths.len());
    let mut progress = Progress::new(inode_to_paths.len());
    // TODO: This nesting is horrible... let's improve on this
    for size_group in same_size_groups(files) {
        if exclude_if_unique(&size_group, &mut progress, "It has unique size.") {
            continue;
        }
        if dedup_if_pair(&inode_to_paths, &size_group, dry_run, &mut progress) {
            continue;
        }
        for prefix_group in same_prefix_groups(size_group) {
            if exclude_if_unique(&prefix_group, &mut progress, "It has a unique prefix.") {
                continue;
            }
            if dedup_if_pair(&inode_to_paths, &prefix_group, dry_run, &mut progress) {
                continue;
            }
            for hash_group in same_hash_groups(prefix_group) {
                if exclude_if_unique(&hash_group, &mut progress, "It has a unique hash.") {
                    continue;
                }
                if paranoid {
                    same_content_dedup(&inode_to_paths, &hash_group, dry_run, &mut progress);
                } else {
                    hardlink_dedup(&inode_to_paths, hash_group, dry_run, &mut progress);
                }
            }
        }
    }
    println!("Estimated saved bytes: {}", progress.bytes_deduped);
}

fn exclude_if_unique<'a>(
    group: &HashSet<&'a PathBuf>,
    progress: &mut Progress,
    uniqueness_msg: &str,
) -> bool {
    if group.len() > 1 {
        return false;
    }
    progress.processed += group.len();
    println!(
        "[{}] Excluding {:?} from deduplication. {}",
        progress,
        group.iter().nth(0).unwrap(),
        uniqueness_msg,
    );
    true
}

fn dedup_if_pair<'a>(
    inode_to_paths: &HashMap<u64, HashSet<PathBuf>>,
    group: &HashSet<&'a PathBuf>,
    dry_run: bool,
    progress: &mut Progress,
) -> bool {
    if group.len() == 2 {
        same_content_dedup(&inode_to_paths, group, dry_run, progress);
        return true;
    }
    false
}

fn same_content_dedup<'a>(
    inode_to_paths: &HashMap<u64, HashSet<PathBuf>>,
    file_group: &HashSet<&'a PathBuf>,
    dry_run: bool,
    progress: &mut Progress,
) {
    for content_group in same_content_groups(file_group) {
        if exclude_if_unique(&content_group, progress, "It has unique contents.") {
        } else {
            hardlink_dedup(&inode_to_paths, content_group, dry_run, progress)
        }
    }
}

fn hardlink_dedup<'a>(
    inode_to_paths: &HashMap<u64, HashSet<PathBuf>>,
    same_files_group: HashSet<&'a PathBuf>,
    dry_run: bool,
    progress: &mut Progress,
) {
    let mut same_files_iterator = same_files_group.iter();
    if let Some(original_file) = same_files_iterator.next() {
        progress.processed += 1;
        while let Some(other_file) = same_files_iterator.next() {
            progress.processed += 1;
            if let Ok(other_file_metadata) = metadata(other_file) {
                hardlink(
                    &original_file,
                    inode_to_paths[&other_file_metadata.ino()].iter(),
                    dry_run,
                    progress,
                );
                progress.bytes_deduped += other_file_metadata.len() as usize;
            }
        }
    }
}

fn hardlink<'a>(
    original_file: &Path,
    targets: impl Iterator<Item = &'a PathBuf>,
    dry_run: bool,
    progress: &Progress,
) {
    for target in targets {
        if dry_run {
            println!(
                "[{}] Would hardlink {:?} to {:?}.",
                progress, original_file, target
            );
            continue;
        }
        let tmp_file = target.parent().unwrap().join(Uuid::new_v4().to_string());
        if let Err(err) = hard_link(original_file, &tmp_file) {
            warn!(
                "Failed to create temprary hardlink of {:?} at {:?}. Error: {}",
                original_file, tmp_file, err
            );
            continue;
        }
        match rename(&tmp_file, target) {
            Ok(_) => println!(
                "[{}] Hardlinked {:?} to {:?}.",
                progress, original_file, target
            ),
            Err(err) => {
                warn!(
                    "Failed to hardlink {:?} to {:?}. Error: {}",
                    original_file, target, err
                );
                if let Err(err) = remove_file(&tmp_file) {
                    warn!(
                        "Failed to delete the temprary file {:?}. Error: {}",
                        tmp_file, err
                    );
                }
            }
        }
    }
}

fn find_inode_groups(paths: &Vec<PathBuf>) -> HashMap<u64, HashSet<PathBuf>> {
    let mut inode_to_paths = HashMap::new();
    for path in paths {
        for file in find_files(path) {
            let same_inode_files = inode_to_paths
                .entry(file.ino())
                .or_insert_with(|| HashSet::new());
            same_inode_files.insert(file.path().to_owned());
        }
    }
    inode_to_paths
}

fn find_files(path: &Path) -> impl Iterator<Item = DirEntry> {
    WalkDir::new(path)
        .into_iter()
        .flatten()
        .filter(|entry| entry.file_type().is_file())
}

fn group_by<'a, TKey>(
    unrefined_group: impl Iterator<Item = &'a PathBuf>,
    to_key: fn(&'a PathBuf) -> Option<TKey>,
) -> impl Iterator<Item = HashSet<&'a PathBuf>>
where
    TKey: std::cmp::Eq + std::hash::Hash,
{
    let mut groups = HashMap::new();
    for file in unrefined_group {
        if let Some(key) = to_key(file) {
            let group = groups.entry(key).or_insert_with(|| HashSet::new());
            group.insert(file);
        }
    }
    groups.into_values()
}

fn same_size_groups<'a>(
    files: impl Iterator<Item = &'a PathBuf>,
) -> impl Iterator<Item = HashSet<&'a PathBuf>> {
    group_by(files, |file| metadata(file).map(|m| m.len()).ok())
}

fn same_prefix_groups<'a>(
    files: HashSet<&'a PathBuf>,
) -> impl Iterator<Item = HashSet<&'a PathBuf>> {
    group_by(files.into_iter(), |file| read_prefix(file).ok())
}

fn same_hash_groups<'a>(files: HashSet<&'a PathBuf>) -> impl Iterator<Item = HashSet<&'a PathBuf>> {
    group_by(files.into_iter(), |file| calculate_hash(file).ok())
}

fn same_content_groups<'a>(files: &HashSet<&'a PathBuf>) -> Vec<HashSet<&'a PathBuf>> {
    let mut files_remaining = files.clone();
    let mut content_groups = Vec::new();
    while files_remaining.len() > 0 {
        let file = files_remaining.iter().cloned().nth(0).unwrap();
        files_remaining.remove(file);
        let mut content_group = find_equal_files(file, &files_remaining);
        files_remaining = files_remaining
            .difference(&content_group)
            .cloned()
            .collect();
        content_group.insert(file);
        content_groups.push(content_group);
    }
    content_groups
}

fn find_equal_files<'a>(file: &Path, other_files: &HashSet<&'a PathBuf>) -> HashSet<&'a PathBuf> {
    let mut equal_files = HashSet::new();
    for other_file in other_files.iter().cloned() {
        match are_files_same(file, other_file) {
            Ok(true) => {
                equal_files.insert(other_file);
            }
            Ok(false) => (),
            Err(err) => warn!(
                "Failed to compare files {:?} and {:?}. Error: {}",
                file, other_file, err
            ),
        };
    }
    equal_files
}

fn are_files_same(file: &Path, other_file: &Path) -> Result<bool> {
    let open_file_1 = File::open(file)?;
    let open_file_2 = File::open(other_file)?;
    let mut reader1 = BufReader::new(open_file_1);
    let mut reader2 = BufReader::new(open_file_2);
    let mut buf1 = [0; 4096];
    let mut buf2 = [0; 4096];
    loop {
        let read_bytes1 = reader1.read(&mut buf1)?;
        let read_bytes2 = reader2.read(&mut buf2)?;
        if read_bytes2 != read_bytes2 {
            return Ok(false);
        }
        if read_bytes1 == 0 {
            return Ok(true);
        }
        if buf1[0..read_bytes1] == buf2[0..read_bytes1] {
            continue;
        }
        return Ok(false);
    }
}

fn read_prefix(file: &Path) -> std::io::Result<Vec<u8>> {
    let mut buffer = vec![0; 64];
    let mut file_handle = File::open(file)?;
    file_handle.read(&mut buffer[..])?;
    Ok(buffer)
}

fn calculate_hash(file: &Path) -> std::io::Result<Vec<u8>> {
    let mut file_handle = File::open(file)?;
    let mut hasher = Sha256::new();
    io::copy(&mut file_handle, &mut hasher)?;
    Ok(hasher.finalize().to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn same_size_group_empty() {
        let mut size_groups = same_size_groups(std::iter::empty());
        assert_eq!(size_groups.next(), None);
    }

    #[test]
    fn one_same_size() {
        let tmp_dir = tempdir().unwrap();
        let file1 = tmp_file(&tmp_dir.path().join("dir1"), "file1", "contents 1");
        let mut size_groups = same_size_groups(vec![&file1].into_iter());
        assert_eq!(size_groups.next().unwrap(), HashSet::from([&file1]));
        assert_eq!(size_groups.next(), None);
    }

    #[test]
    fn two_same_size() {
        let tmp_dir = tempdir().unwrap();
        let file1 = tmp_file(&tmp_dir.path().join("dir1"), "file1", "contents 1");
        let file2 = tmp_file(&tmp_dir.path().join("dir2"), "file2", "contents 2");
        let mut size_groups = same_size_groups(vec![&file1, &file2].into_iter());
        assert_eq!(size_groups.next().unwrap(), HashSet::from([&file1, &file2]));
        assert_eq!(size_groups.next(), None);
    }

    #[test]
    fn two_same_size_one_different() {
        let tmp_dir = tempdir().unwrap();
        let file1 = tmp_file(&tmp_dir.path().join("dir1"), "file1", "contents 1");
        let file2 = tmp_file(&tmp_dir.path().join("dir2"), "file2", "contents 2");
        let smaller_file = tmp_file(&tmp_dir.path().join("dir3"), "smaller_file", "smaller");
        let size_groups: Vec<HashSet<&PathBuf>> =
            same_size_groups(vec![&file1, &file2, &smaller_file].into_iter()).collect();
        assert!(size_groups.contains(&HashSet::from([&file1, &file2])));
        assert!(size_groups.contains(&HashSet::from([&smaller_file])));
        assert_eq!(size_groups.len(), 2);
    }

    #[test]
    fn two_same_prefix_one_different() {
        let tmp_dir = tempdir().unwrap();
        let file1 = tmp_file(&tmp_dir.path().join("dir1"), "file1", "same prefix");
        let file2 = tmp_file(&tmp_dir.path().join("dir2"), "file2", "same prefix");
        let smaller_file = tmp_file(&tmp_dir.path().join("dir3"), "smaller_file", "smaller");
        let prefix_groups: Vec<HashSet<&PathBuf>> =
            same_prefix_groups(HashSet::from([&file1, &file2, &smaller_file])).collect();
        assert!(prefix_groups.contains(&HashSet::from([&file1, &file2])));
        assert!(prefix_groups.contains(&HashSet::from([&smaller_file])));
        assert_eq!(prefix_groups.len(), 2);
    }

    #[test]
    fn two_same_hash_one_different() {
        let tmp_dir = tempdir().unwrap();
        let file1 = tmp_file(&tmp_dir.path().join("dir1"), "file1", "same content");
        let file2 = tmp_file(&tmp_dir.path().join("dir2"), "file2", "same content");
        let smaller_file = tmp_file(&tmp_dir.path().join("dir3"), "smaller_file", "smaller");
        let hash_groups: Vec<HashSet<&PathBuf>> =
            same_hash_groups(HashSet::from([&file1, &file2, &smaller_file])).collect();
        assert!(hash_groups.contains(&HashSet::from([&file1, &file2])));
        assert!(hash_groups.contains(&HashSet::from([&smaller_file])));
        assert_eq!(hash_groups.len(), 2);
    }

    fn tmp_file(dir: &Path, file_name: &str, contents: &str) -> PathBuf {
        std::fs::create_dir_all(dir).unwrap();
        let path = dir.join(file_name);
        let mut file = File::create(&path).unwrap();
        file.write_all(contents.as_bytes()).unwrap();
        return path;
    }
}
