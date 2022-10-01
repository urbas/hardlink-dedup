from dataclasses import dataclass
import hashlib
import itertools
import logging
import subprocess
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Tuple, NamedTuple, Set, TypeVar

import click

EqClassMember = TypeVar("EqClassMember")


class FileInfo(NamedTuple):
    path: Path
    device_number: int
    inode: int
    size: int


class InodeEqClass(NamedTuple):
    device_number: int
    inode: int


class SizeEqClass(NamedTuple):
    device_number: int
    size: int


class HashEqClass(NamedTuple):
    size_eq_class: SizeEqClass
    size: bytes


@dataclass()
class ProgressCounter:
    remaining: int
    actual: int = 0


@click.command()
@click.option("-v", "--verbosity", count=True)
@click.option("-n", "--dry-run", count=True)
@click.argument("paths", nargs=-1, type=click.Path(exists=True, path_type=Path))
def main(verbosity: int, dry_run: bool, paths: Tuple[Path]) -> None:
    logging.basicConfig(
        format="[%(levelname)s %(asctime)s] %(message)s",
        level=logging.WARNING - 10 * verbosity,
    )
    dedup(paths, dry_run)


def dedup(paths: Tuple[Path], dry_run: bool = False) -> None:
    file_infos = list(get_file_infos(paths))
    logging.info("All files: %d", len(file_infos))

    inode_eq_classes = get_inode_eq_classes(file_infos)
    size_eq_classes = get_size_eq_classes(inode_eq_classes)
    hash_eq_classes = get_hash_eq_classes(inode_eq_classes, size_eq_classes)
    content_eq_classes = get_content_eq_classes(
        inode_eq_classes, hash_eq_classes.values()
    )
    hardlink(inode_eq_classes, content_eq_classes, dry_run)
    report(inode_eq_classes, content_eq_classes)


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
        found_paths = fd_result.stdout.decode("utf-8").split("\0")
        logging.debug("Found %s files in '%s'.", len(found_paths), path)
        for path_str in found_paths:
            if not path_str:
                continue
            current_path = Path(path_str)
            file_stat = current_path.stat()
            yield FileInfo(
                current_path,
                file_stat.st_dev,
                file_stat.st_ino,
                file_stat.st_size,
            )


def get_inode_eq_classes(
    file_infos: Iterable[FileInfo],
) -> Dict[InodeEqClass, Set[FileInfo]]:
    inode_eq_classes: Dict[InodeEqClass, Set[FileInfo]] = dict()
    for file_info in file_infos:
        inode_eq_class = InodeEqClass(file_info.device_number, file_info.inode)
        eq_class_members = inode_eq_classes.get(inode_eq_class)
        if eq_class_members is None:
            eq_class_members = set()
            inode_eq_classes[inode_eq_class] = eq_class_members
        eq_class_members.add(file_info)

    logging.info(
        "Inode classes: %s (number of groups of files on the same device that share the same inode)",
        len(inode_eq_classes),
    )

    return inode_eq_classes


def get_size_eq_classes(
    inode_eq_classes: Dict[InodeEqClass, Set[FileInfo]]
) -> Dict[SizeEqClass, Set[InodeEqClass]]:
    size_eq_classes: Dict[SizeEqClass, Set[InodeEqClass]] = dict()
    for inode_eq_class, inode_eq_class_members in inode_eq_classes.items():
        file_info = representative(inode_eq_class_members)
        size_eq_class = SizeEqClass(inode_eq_class.device_number, file_info.size)
        size_eq_class_members = size_eq_classes.get(size_eq_class)
        if size_eq_class_members is None:
            size_eq_class_members = set()
            size_eq_classes[size_eq_class] = size_eq_class_members
        size_eq_class_members.add(inode_eq_class)

    non_trivial_size_eq_classes = {
        size_eq_class: size_eq_class_members
        for size_eq_class, size_eq_class_members in size_eq_classes.items()
        if len(size_eq_class_members) > 1
    }

    logging.info(
        "Excluded by size: %d (number of inode classes excluded from deduplication because they have unique file sizes)",
        len(size_eq_classes) - len(non_trivial_size_eq_classes),
    )

    return non_trivial_size_eq_classes


def get_hash_eq_classes(
    inode_eq_classes: Dict[InodeEqClass, Set[FileInfo]],
    size_eq_classes: Dict[SizeEqClass, Set[InodeEqClass]],
) -> Dict[HashEqClass, Set[InodeEqClass]]:
    hash_eq_classes: Dict[HashEqClass, Set[InodeEqClass]] = dict()
    progress_counter = ProgressCounter(
        remaining=sum(
            len(size_eq_class_members)
            for size_eq_class_members in size_eq_classes.values()
        )
    )
    for size_eq_class, size_eq_class_members in size_eq_classes.items():
        for inode_eq_class in size_eq_class_members:
            progress_counter.actual += 1
            file_info = representative(inode_eq_classes[inode_eq_class])
            logging.debug(
                "Calculating hash %d out of %d. File size %d. File: '%s'.",
                progress_counter.actual,
                progress_counter.remaining,
                size_eq_class.size,
                str(file_info.path),
            )
            file_hash = hashlib.md5(file_info.path.read_bytes()).digest()
            hash_eq_class = HashEqClass(size_eq_class, file_hash)
            hash_eq_class_members = hash_eq_classes.get(hash_eq_class)
            if hash_eq_class_members is None:
                hash_eq_class_members = set()
                hash_eq_classes[hash_eq_class] = hash_eq_class_members
            hash_eq_class_members.add(inode_eq_class)

    non_trivial_hash_eq_classes = {
        hash_eq_class: hash_eq_class_members
        for hash_eq_class, hash_eq_class_members in hash_eq_classes.items()
        if len(hash_eq_class_members) > 1
    }

    logging.info(
        "Excluded by hash: %d (number of inode classes excluded from deduplication because they have unique hashes)",
        len(hash_eq_classes) - len(non_trivial_hash_eq_classes),
    )

    return non_trivial_hash_eq_classes


def get_content_eq_classes(
    inode_eq_classes: Dict[InodeEqClass, Set[FileInfo]],
    heuristic_eq_classes: Iterable[Set[InodeEqClass]],
) -> List[Set[InodeEqClass]]:
    """
    Constructs equivalence classes of inodes that have exactly the same content.

    Heuristic equivalence classes must be strictly more relaxed than content equivalence classes.
    This means that if two inodes have the same contents, then they must be in the same heuristic
    equivalence class (but the other way does not have to hold). This helps us make fewer costly
    comparisons.
    """
    # we make a copy of the iterable becaues we have to traverse it multiple times
    heuristic_eq_classes = list(heuristic_eq_classes)
    comparison_counter = ProgressCounter(
        remaining=sum(
            len(eq_class) * (len(eq_class) + 1) // 2
            for eq_class in heuristic_eq_classes
        ),
    )
    logging.debug(
        "Expecting a maximum of %d pairwise file content comparisons (byte-by-byte). This might take a while.",
        comparison_counter.remaining,
    )
    content_eq_classes: List[Set[InodeEqClass]] = list()
    for eq_class_members in heuristic_eq_classes:
        remaining_inodes = eq_class_members.copy()
        while remaining_inodes:
            current_inode = remaining_inodes.pop()
            equal_inodes = set(
                find_equal_inodes(
                    inode_eq_classes,
                    current_inode,
                    remaining_inodes,
                    comparison_counter,
                )
            )
            remaining_inodes.difference_update(equal_inodes)
            content_eq_class = {current_inode}
            content_eq_class.update(equal_inodes)
            content_eq_classes.append(content_eq_class)
        comparison_counter.remaining -= (
            len(eq_class_members) * (len(eq_class_members) + 1) // 2
        )

    non_trivial_content_eq_classes = [
        content_eq_class
        for content_eq_class in content_eq_classes
        if len(content_eq_class) > 1
    ]

    logging.info(
        "Excluded by content: %d (number of inode classes excluded from deduplication because they have unique contents)",
        len(content_eq_classes) - len(non_trivial_content_eq_classes),
    )

    return non_trivial_content_eq_classes


def report(
    inode_eq_classes: Dict[InodeEqClass, Set[FileInfo]],
    content_eq_classes: Iterable[Set[InodeEqClass]],
) -> None:
    total_space_saved = 0
    for content_eq_class in content_eq_classes:
        representative_file = representative(
            inode_eq_classes[representative(content_eq_class)]
        )
        total_space_saved += (len(content_eq_class) - 1) * representative_file.size
    logging.info(f"Expected reduction by {total_space_saved:,} bytes")


def hardlink(
    inode_eq_classes: Dict[InodeEqClass, Set[FileInfo]],
    content_eq_classes: List[Set[InodeEqClass]],
    dry_run: bool,
) -> None:
    hardlinks_done = 0
    for content_eq_class in content_eq_classes:
        remaining_eq_inodes = content_eq_class.copy()
        original_file = representative(inode_eq_classes[remaining_eq_inodes.pop()])
        paths_to_replace = set(
            file_info.path
            for file_info in itertools.chain.from_iterable(
                inode_eq_classes[eq_inode] for eq_inode in remaining_eq_inodes
            )
        )
        for path_to_replace in paths_to_replace:
            logging.debug(
                "%s '%s' to '%s'.",
                "Would hardlink" if dry_run else "Hardlinking",
                str(original_file.path),
                path_to_replace,
            )
            hardlinks_done += 1
            if dry_run:
                continue
            subprocess.run(
                ["ln", "-f", original_file.path, path_to_replace], check=True
            )
    logging.debug(
        "%s %d files.", "Would hardlink" if dry_run else "Hardlinked", hardlinks_done
    )


def find_equal_inodes(
    inode_eq_classes: Dict[InodeEqClass, Set[FileInfo]],
    inode_eq_class: InodeEqClass,
    other_inode_eq_classes: Set[InodeEqClass],
    comparison_counter: ProgressCounter,
) -> Iterator[InodeEqClass]:
    this_file = representative(inode_eq_classes[inode_eq_class])
    for other_inode in other_inode_eq_classes:
        other_file = representative(inode_eq_classes[other_inode])
        comparison_counter.actual += 1
        logging.debug(
            "[%.1f%%] Comparing files '%s' and '%s'...",
            100
            * comparison_counter.actual
            / (comparison_counter.actual + comparison_counter.remaining),
            this_file.path,
            other_file.path,
        )
        cmp_result = subprocess.run(
            ["cmp", "--quiet", this_file.path, other_file.path],
            text=True,
            capture_output=True,
            check=False,
        )
        if cmp_result.returncode == 0:
            yield other_inode


def representative(eq_class_members: Set[EqClassMember]) -> EqClassMember:
    return next(iter(eq_class_members))
