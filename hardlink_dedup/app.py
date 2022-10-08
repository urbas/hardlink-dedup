from dataclasses import dataclass
import hashlib
import itertools
import logging
import subprocess
from pathlib import Path
from typing import (
    Callable,
    Dict,
    Iterable,
    Iterator,
    Tuple,
    NamedTuple,
    Set,
    TypeVar,
)

import click

TKey = TypeVar("TKey")


class Inode(NamedTuple):
    device_number: int
    inode: int
    size: int


class FileInfo(NamedTuple):
    path: Path
    inode: Inode


@dataclass()
class ProgressCounter:
    remaining: int
    actual: int = 0

    def percent(self) -> float:
        if self.remaining == 0:
            return 100
        return self.actual / self.remaining * 100


@click.command()
@click.option(
    "-v",
    "--verbosity",
    count=True,
    help="Repeat this flag to increase verbosity: `-v` for progress, `-vv` for detailed operations.",
)
@click.option("-n", "--dry-run", count=True)
@click.argument("paths", nargs=-1, type=click.Path(exists=True, path_type=Path))
def main(verbosity: int, dry_run: bool, paths: Tuple[Path]) -> None:
    logging.basicConfig(
        format="[%(levelname)s %(asctime)s] %(message)s",
        level=logging.WARNING - 10 * verbosity,
    )
    dedup(paths, dry_run)


def dedup(paths: Tuple[Path], dry_run: bool = False) -> None:
    inodes_to_files = group_by_inodes(get_file_infos(paths))
    progress = ProgressCounter(len(inodes_to_files))
    click.echo(f"Total files to compare: {len(inodes_to_files)}")
    files = (next(iter(files)) for files in inodes_to_files.values())
    size_groups = exclude_unique(same_size_groups(files), progress, unique_by="size")
    prefix_groups = exclude_unique(
        pairs_passthru(same_prefix_groups, size_groups), progress, unique_by="prefix"
    )
    hash_groups = exclude_unique(
        pairs_passthru(same_hash_groups, prefix_groups), progress, unique_by="hash"
    )
    content_groups = exclude_unique(
        same_content_groups(hash_groups), progress, unique_by="content"
    )
    hardlink(inodes_to_files, content_groups, dry_run, progress)


def get_file_infos(paths: Iterable[Path]) -> Iterable[FileInfo]:
    for path in paths:
        fd_result = subprocess.run(
            [
                "fd",
                "--print0",
                "--type",
                "file",
                ".",
                path,
            ],
            capture_output=True,
            check=True,
        )
        found_paths = 0
        for path_str in fd_result.stdout.decode("utf-8").split("\0"):
            if not path_str:
                continue
            found_paths += 1
            current_path = Path(path_str)
            file_stat = current_path.stat(follow_symlinks=False)
            yield FileInfo(
                current_path,
                inode=Inode(
                    file_stat.st_dev,
                    file_stat.st_ino,
                    file_stat.st_size,
                ),
            )
        logging.info("Found %s files in '%s'.", found_paths, path)


def group_by_inodes(
    file_infos: Iterable[FileInfo],
) -> Dict[Inode, Set[FileInfo]]:
    inodes_to_files: Dict[Inode, Set[FileInfo]] = dict()
    for file_info in file_infos:
        inode = file_info.inode
        same_inode_files = inodes_to_files.get(inode)
        if same_inode_files is None:
            same_inode_files = set()
            inodes_to_files[inode] = same_inode_files
        same_inode_files.add(file_info)

    logging.info(
        "Inodes: %s (number of groups of files on the same device that share the same inode)",
        len(inodes_to_files),
    )

    return inodes_to_files


def refine_group(
    file_group: Iterable[FileInfo],
    key: Callable[[FileInfo], TKey],
) -> Iterable[Set[FileInfo]]:
    refined_groups: Dict[TKey, Set[FileInfo]] = {}
    for file_info in file_group:
        refined_group_key = key(file_info)
        refined_file_infos = refined_groups.get(refined_group_key)
        if refined_file_infos is None:
            refined_file_infos = set()
            refined_groups[refined_group_key] = refined_file_infos
        refined_file_infos.add(file_info)
    yield from refined_groups.values()


def exclude_unique(
    file_groups: Iterable[Set[FileInfo]],
    progress: ProgressCounter,
    unique_by: str,
) -> Iterable[Set[FileInfo]]:
    for file_group in file_groups:
        if len(file_group) == 1:
            progress.actual += 1
            logging.info(
                "[%.2f%%] Excluding '%s' from deduplication. We know it's unique by %s.",
                progress.percent(),
                next(iter(file_group)).path,
                unique_by,
            )
            continue
        yield file_group


def pairs_passthru(
    refining: Callable[[Set[FileInfo]], Iterable[Set[FileInfo]]],
    file_groups: Iterable[Set[FileInfo]],
) -> Iterable[Set[FileInfo]]:
    return itertools.chain.from_iterable(
        [file_group] if len(file_group) == 2 else refining(file_group)
        for file_group in file_groups
    )


def same_size_groups(file_group: Iterable[FileInfo]) -> Iterable[Set[FileInfo]]:
    return refine_group(
        file_group,
        key=lambda file_info: (file_info.inode.device_number, file_info.inode.size),
    )


def same_prefix_groups(file_group: Set[FileInfo]) -> Iterable[Set[FileInfo]]:
    return refine_group(file_group, key=read_prefix)


def same_hash_groups(
    file_group: Set[FileInfo],
) -> Iterable[Set[FileInfo]]:
    def _hash(file_info: FileInfo):
        logging.debug(
            "Hashing file '%s' (size: %d bytes)", file_info.path, file_info.inode.size
        )
        return hashlib.md5(file_info.path.read_bytes()).digest()

    return refine_group(file_group, key=_hash)


def same_content_groups(
    file_groups: Iterable[Set[FileInfo]],
) -> Iterable[Set[FileInfo]]:
    for file_group in file_groups:
        remaining_files = file_group.copy()
        while remaining_files:
            current_file = remaining_files.pop()
            content_group = set(find_equal_files(current_file, remaining_files))
            remaining_files.difference_update(content_group)
            content_group.add(current_file)
            yield content_group


def hardlink(
    inodes_to_files: Dict[Inode, Set[FileInfo]],
    content_groups: Iterable[Set[FileInfo]],
    dry_run: bool,
    progress: ProgressCounter,
) -> None:
    hardlinks_done = 0
    total_size_dedup = 0
    for content_group in content_groups:
        progress.actual += 1
        content_group = content_group.copy()
        original_file = content_group.pop()
        for other_file_info in content_group:
            total_size_dedup += other_file_info.inode.size
            progress.actual += 1
            for file_to_replace in inodes_to_files[other_file_info.inode]:
                logging.info(
                    "[%.2f%%]  %s '%s' to '%s'.",
                    progress.percent(),
                    "Would hardlink" if dry_run else "Hardlinking",
                    str(original_file.path),
                    file_to_replace.path,
                )
                hardlinks_done += 1
                if dry_run:
                    continue
                subprocess.run(
                    ["ln", "-f", original_file.path, file_to_replace.path], check=True
                )
    click.echo(
        f"{'Files to hardlink' if dry_run else 'Hardlinked files'}: {hardlinks_done}"
    )
    click.echo(f"Estimated saved bytes: {total_size_dedup}")


def find_equal_files(
    this_file: FileInfo,
    other_files: Iterable[FileInfo],
) -> Iterator[FileInfo]:
    for other_file in other_files:
        logging.debug(
            "Comparing files '%s' and '%s' (both of size: %d bytes)",
            this_file.path,
            other_file.path,
            this_file.inode.size,
        )
        cmp_result = subprocess.run(
            ["cmp", "--quiet", this_file.path, other_file.path],
            text=True,
            capture_output=True,
            check=False,
        )
        if cmp_result.returncode == 0:
            yield other_file


def read_prefix(file_info: FileInfo) -> bytes:
    with file_info.path.open("rb") as file_handle:
        return file_handle.read(min(file_info.inode.size, 64))
