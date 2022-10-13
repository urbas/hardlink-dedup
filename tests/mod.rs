pub mod test_utils;

use assert_cmd::prelude::*;
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

fn dedup(paths: &[&str]) -> assert_cmd::assert::Assert {
    let mut cmd = Command::cargo_bin("hardlink-dedup").unwrap();
    let cmd_with_args = cmd.args(paths);
    println!("Running cmd: {:?}", cmd_with_args);
    let output = cmd_with_args.unwrap();
    println!("Output: {:?}", output);
    return output.assert();
}
