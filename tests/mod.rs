pub mod test_utils;

use assert_cmd::prelude::*;
use nix::unistd::{chown, getgroups, Gid};
use std::fs::{metadata, set_permissions};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::process::Command;
use tempfile::tempdir;
use test_utils::{same, tmp_file};

#[test]
fn no_dedup_different_size_files() {
    let tmp_dir = tempdir().unwrap();
    let file1 = tmp_file(&tmp_dir.path().join("dir1"), "file1", "contents 1");
    let file2 = tmp_file(&tmp_dir.path().join("dir2"), "file2", "smaller 2");

    dedup(&[&file1.to_str().unwrap(), &file2.to_str().unwrap()]).success();

    assert!(
        !same(&file1, &file2),
        "Files {:?} and {:?} should stay different.",
        file1,
        file2,
    );
}

#[test]
fn no_dedup_different_files() {
    let tmp_dir = tempdir().unwrap();
    let file1 = tmp_file(&tmp_dir.path().join("dir1"), "file1", "contents 1");
    let file2 = tmp_file(&tmp_dir.path().join("dir2"), "file2", "contents 2");

    dedup(&[&file1.to_str().unwrap(), &file2.to_str().unwrap()]).success();

    assert!(
        !same(&file1, &file2),
        "Files {:?} and {:?} should stay different.",
        file1,
        file2,
    );
}

#[test]
fn dedup_same_files() {
    let tmp_dir = tempdir().unwrap();
    let file1 = tmp_file(&tmp_dir.path().join("dir1"), "file1", "same contents");
    let file2 = tmp_file(&tmp_dir.path().join("dir2"), "file2", "same contents");

    dedup(&[&file1.to_str().unwrap(), &file2.to_str().unwrap()]).success();

    assert!(
        same(&file1, &file2),
        "Files {:?} and {:?} should have been deduped.",
        file1,
        file2,
    );
}

#[test]
fn dedup_same_3_files() {
    let tmp_dir = tempdir().unwrap();
    let file1 = tmp_file(&tmp_dir.path().join("dir1"), "file1", "same contents");
    let file2 = tmp_file(&tmp_dir.path().join("dir2"), "file2", "same contents");
    let file3 = tmp_file(&tmp_dir.path().join("dir3"), "file3", "same contents");

    dedup(&[&tmp_dir.path().to_str().unwrap()]).success();

    assert!(
        same(&file1, &file2),
        "Files {:?} and {:?} should have been deduped.",
        file1,
        file2,
    );

    assert!(
        same(&file1, &file3),
        "Files {:?} and {:?} should have been deduped.",
        file1,
        file3,
    );
}

#[test]
fn dedup_by_prefix() {
    let tmp_dir = tempdir().unwrap();
    let file1 = tmp_file(
        &tmp_dir.path().join("dir1"),
        "file1",
        "same size, same prefix, same content",
    );
    let file2 = tmp_file(
        &tmp_dir.path().join("dir2"),
        "file2",
        "same size, same prefix, same content",
    );
    let file3 = tmp_file(
        &tmp_dir.path().join("dir3"),
        "file3",
        "same size, but different prefix 1234",
    );

    dedup(&[&tmp_dir.path().to_str().unwrap()]).success();

    assert!(
        same(&file1, &file2),
        "Files {:?} and {:?} should have been deduped.",
        file1,
        file2,
    );

    assert!(
        !same(&file1, &file3),
        "Files {:?} and {:?} should not have been deduped.",
        file1,
        file3,
    );
}

#[test]
fn no_dedup_on_dry_run() {
    let tmp_dir = tempdir().unwrap();
    let file1 = tmp_file(&tmp_dir.path().join("dir1"), "file1", "same contents");
    let file2 = tmp_file(&tmp_dir.path().join("dir2"), "file2", "same contents");

    dedup(&[
        "--dry-run",
        &file1.to_str().unwrap(),
        &file2.to_str().unwrap(),
    ])
    .success();

    assert!(
        !same(&file1, &file2),
        "Files {:?} and {:?} should not been deduped.",
        file1,
        file2,
    );
}

#[test]
fn dedup_paranoid() {
    let tmp_dir = tempdir().unwrap();
    let file1 = tmp_file(&tmp_dir.path().join("dir1"), "file1", "same contents");
    let file2 = tmp_file(&tmp_dir.path().join("dir2"), "file2", "same contents");
    let file3 = tmp_file(&tmp_dir.path().join("dir3"), "file3", "same contents");

    dedup(&["--paranoid", &tmp_dir.path().to_str().unwrap()]).success();

    assert!(
        same(&file1, &file2),
        "Files {:?} and {:?} should have been deduped.",
        file1,
        file2,
    );

    assert!(
        same(&file1, &file3),
        "Files {:?} and {:?} should have been deduped.",
        file1,
        file3,
    );
}

#[test]
fn dedup_only_same_permissions() {
    let tmp_dir = tempdir().unwrap();
    let file1 = tmp_file(&tmp_dir.path().join("dir1"), "file1", "same content");
    let file2 = tmp_file(&tmp_dir.path().join("dir2"), "file2", "same content");
    let old_file1_mode = metadata(&file1).unwrap().permissions().mode();
    let mut file2_permissions = metadata(&file2).unwrap().permissions();
    file2_permissions.set_mode(0o100750);
    set_permissions(&file2, file2_permissions).expect("could not set permissions");

    dedup(&[&tmp_dir.path().to_str().unwrap()]).success();

    assert_eq!(metadata(&file2).unwrap().permissions().mode(), 0o100750);
    assert_eq!(
        metadata(&file1).unwrap().permissions().mode(),
        old_file1_mode
    );
    assert!(!same(&file1, &file2));
}

#[test]
fn dedup_only_same_gid() {
    let tmp_dir = tempdir().unwrap();
    let file1 = tmp_file(&tmp_dir.path().join("dir1"), "file1", "same content");
    let file2 = tmp_file(&tmp_dir.path().join("dir2"), "file2", "same content");
    let old_file1_gid = Gid::from_raw(metadata(&file2).unwrap().gid());
    let old_file2_gid = Gid::from_raw(metadata(&file2).unwrap().gid());
    let new_gid = getgroups()
        .unwrap()
        .into_iter()
        .filter(|gid| gid != &old_file2_gid)
        .next()
        .expect("Could not find another group to use for testing.");
    chown(&file2, None, Some(new_gid)).expect("could not change group");

    dedup(&[&tmp_dir.path().to_str().unwrap()]).success();

    assert_eq!(Gid::from_raw(metadata(&file2).unwrap().gid()), new_gid);
    assert_eq!(
        Gid::from_raw(metadata(&file1).unwrap().gid()),
        old_file1_gid
    );
    assert!(!same(&file1, &file2));
}

fn dedup(paths: &[&str]) -> assert_cmd::assert::Assert {
    let mut cmd = Command::cargo_bin("hardlink-dedup").unwrap();
    let cmd_with_args = cmd.args(paths);
    println!("Running cmd: {:?}", cmd_with_args);
    let output = cmd_with_args.unwrap();
    println!("Output: {:?}", output);
    return output.assert();
}
