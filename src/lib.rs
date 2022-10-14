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

struct DedupContext<'a> {
    dry_run: bool,
    total: usize,
    processed: usize,
    bytes_deduped: usize,
    inode_to_paths: &'a HashMap<u64, HashSet<PathBuf>>,
}

impl<'a> DedupContext<'a> {
    fn new(inode_to_paths: &'a HashMap<u64, HashSet<PathBuf>>, dry_run: bool) -> DedupContext {
        DedupContext {
            dry_run: dry_run,
            total: inode_to_paths.len(),
            processed: 0,
            bytes_deduped: 0,
            inode_to_paths: inode_to_paths,
        }
    }
}

impl<'a> std::fmt::Display for DedupContext<'a> {
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
            "{:.2}%{}; {} bytes deduped",
            percentage,
            if self.dry_run { "; dry run" } else { "" },
            self.bytes_deduped,
        )
    }
}

pub fn dedup(paths: &Vec<PathBuf>, dry_run: bool, paranoid: bool) {
    let inode_to_paths = find_inode_groups(paths);
    let mut ctx = DedupContext::new(&inode_to_paths, dry_run);
    println!("Processing {} files.", ctx.total);
    let files = inode_to_paths
        .values()
        .map(|file_group| file_group.iter().nth(0))
        .flatten();
    for size_group in same_metadata_groups(files) {
        if exclude_if_unique(
            &size_group,
            &mut ctx,
            "It has unique size, uid, gid, or mode.",
        ) {
            continue;
        }
        if dedup_if_pair(&size_group, &mut ctx) {
            continue;
        }
        for prefix_group in same_prefix_groups(size_group) {
            if exclude_if_unique(&prefix_group, &mut ctx, "It has a unique prefix.") {
                continue;
            }
            if dedup_if_pair(&prefix_group, &mut ctx) {
                continue;
            }
            for hash_group in same_hash_groups(prefix_group) {
                if exclude_if_unique(&hash_group, &mut ctx, "It has a unique hash.") {
                    continue;
                }
                if paranoid {
                    same_content_dedup(&hash_group, &mut ctx);
                } else {
                    hardlink_dedup(hash_group, &mut ctx);
                }
            }
        }
    }
    println!("Estimated saved bytes: {}", ctx.bytes_deduped);
}

fn exclude_if_unique<'a>(
    group: &HashSet<&'a PathBuf>,
    ctx: &mut DedupContext,
    uniqueness_msg: &str,
) -> bool {
    if group.len() > 1 {
        return false;
    }
    ctx.processed += group.len();
    println!(
        "[{}] Excluding {:?} from deduplication. {}",
        ctx,
        group.iter().nth(0).unwrap(),
        uniqueness_msg,
    );
    true
}

/// If we have a pair of same-sized files, it's faster to compare them byte-for-byte
/// rather than calculate their hashes and compare hashes.
fn dedup_if_pair<'a>(group: &HashSet<&'a PathBuf>, ctx: &mut DedupContext) -> bool {
    if group.len() == 2 {
        same_content_dedup(group, ctx);
        return true;
    }
    false
}

fn same_content_dedup<'a>(file_group: &HashSet<&'a PathBuf>, ctx: &mut DedupContext) {
    for content_group in same_content_groups(file_group) {
        if exclude_if_unique(&content_group, ctx, "It has unique contents.") {
            continue;
        }
        hardlink_dedup(content_group, ctx)
    }
}

fn hardlink_dedup<'a>(same_files_group: HashSet<&'a PathBuf>, ctx: &mut DedupContext) {
    let mut same_files_iterator = same_files_group.iter();
    if let Some(original_file) = same_files_iterator.next() {
        ctx.processed += 1;
        while let Some(other_file) = same_files_iterator.next() {
            ctx.processed += 1;
            if let Ok(other_file_metadata) = metadata(other_file) {
                replace_many_with_hard_link(
                    &original_file,
                    ctx.inode_to_paths[&other_file_metadata.ino()].iter(),
                    ctx.dry_run,
                    ctx,
                );
                ctx.bytes_deduped += other_file_metadata.len() as usize;
            }
        }
    }
}

fn replace_many_with_hard_link<'a>(
    original_file: &Path,
    targets: impl Iterator<Item = &'a PathBuf>,
    dry_run: bool,
    ctx: &DedupContext,
) {
    for target in targets {
        if dry_run {
            println!(
                "[{}] Would hardlink {:?} to {:?}.",
                ctx, original_file, target
            );
            continue;
        }
        match replace_with_hard_link(original_file, target) {
            Ok(_) => println!("[{}] Hardlinked {:?} to {:?}.", ctx, original_file, target),
            Err(err) => warn!(
                "Failed to hardlink {:?} to {:?}. Error: {}",
                original_file, target, err
            ),
        }
    }
}

fn replace_with_hard_link(original_file: &Path, target: &Path) -> std::result::Result<(), String> {
    let tmp_file = target.parent().unwrap().join(Uuid::new_v4().to_string());
    let _ = hard_link(original_file, &tmp_file).map_err(|err| {
        format!(
            "Failed to create temporary hardlink of {:?} at {:?}. Error: {}",
            original_file, tmp_file, err
        )
    })?;
    rename(&tmp_file, target)
        .map_err(|err| {
            format!(
                "Failed to replace target file {:?} with temporary hardlink {:?}. Error: {}",
                target, tmp_file, err
            )
        })
        .map_err(|err| {
            if let Err(inner_err) = remove_file(&tmp_file) {
                format!(
                    "After error '{}' also failed to delete temporary file {:?} with error: {}",
                    err, tmp_file, inner_err
                )
            } else {
                err
            }
        })
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

fn same_metadata_groups<'a>(
    files: impl Iterator<Item = &'a PathBuf>,
) -> impl Iterator<Item = HashSet<&'a PathBuf>> {
    group_by(files, |file| {
        metadata(file)
            .map(|m| (m.len(), m.gid(), m.uid(), m.mode()))
            .map_err(|err| {
                warn!(
                    "Skipping file {:?}. Failed to fetch its metadata. Error: {}",
                    file, err
                )
            })
            .ok()
    })
}

fn same_prefix_groups<'a>(
    files: HashSet<&'a PathBuf>,
) -> impl Iterator<Item = HashSet<&'a PathBuf>> {
    group_by(files.into_iter(), |file| {
        read_prefix(file)
            .map_err(|err| {
                warn!(
                    "Skipping file {:?}. Failed to read its first few bytes. Error: {}",
                    file, err
                )
            })
            .ok()
    })
}

fn same_hash_groups<'a>(files: HashSet<&'a PathBuf>) -> impl Iterator<Item = HashSet<&'a PathBuf>> {
    group_by(files.into_iter(), |file| {
        calculate_hash(file)
            .map_err(|err| {
                warn!(
                    "Skipping file {:?}. Failed to calculate its hash. Error: {}",
                    file, err
                )
            })
            .ok()
    })
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
        let mut size_groups = same_metadata_groups(std::iter::empty());
        assert_eq!(size_groups.next(), None);
    }

    #[test]
    fn one_same_size() {
        let tmp_dir = tempdir().unwrap();
        let file1 = tmp_file(&tmp_dir.path().join("dir1"), "file1", "contents 1");
        let mut size_groups = same_metadata_groups(vec![&file1].into_iter());
        assert_eq!(size_groups.next().unwrap(), HashSet::from([&file1]));
        assert_eq!(size_groups.next(), None);
    }

    #[test]
    fn two_same_size() {
        let tmp_dir = tempdir().unwrap();
        let file1 = tmp_file(&tmp_dir.path().join("dir1"), "file1", "contents 1");
        let file2 = tmp_file(&tmp_dir.path().join("dir2"), "file2", "contents 2");
        let mut size_groups = same_metadata_groups(vec![&file1, &file2].into_iter());
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
            same_metadata_groups(vec![&file1, &file2, &smaller_file].into_iter()).collect();
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

    #[test]
    fn two_same_content_one_different() {
        let tmp_dir = tempdir().unwrap();
        let file1 = tmp_file(&tmp_dir.path().join("dir1"), "file1", "same content");
        let file2 = tmp_file(&tmp_dir.path().join("dir2"), "file2", "same content");
        let smaller_file = tmp_file(&tmp_dir.path().join("dir3"), "smaller_file", "smaller");
        let content_groups: Vec<HashSet<&PathBuf>> =
            same_content_groups(&HashSet::from([&file1, &file2, &smaller_file]));
        assert!(content_groups.contains(&HashSet::from([&file1, &file2])));
        assert!(content_groups.contains(&HashSet::from([&smaller_file])));
        assert_eq!(content_groups.len(), 2);
    }

    #[test]
    fn replace_with_hardlink_same() {
        let tmp_dir = tempdir().unwrap();
        let file1 = tmp_file(&tmp_dir.path().join("dir1"), "file1", "same content");
        let file2 = tmp_file(&tmp_dir.path().join("dir2"), "file2", "same content");
        let hard_link_result = replace_with_hard_link(&file1, &file2);
        assert_eq!(hard_link_result.unwrap(), ());
        assert!(same(&file1, &file2));
    }

    fn tmp_file(dir: &Path, file_name: &str, contents: &str) -> PathBuf {
        std::fs::create_dir_all(dir).unwrap();
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
}
