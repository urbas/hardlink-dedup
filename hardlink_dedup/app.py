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
    List,
    Tuple,
    NamedTuple,
    Set,
    TypeVar,
)

import click

EqClassId = TypeVar("EqClassId")
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


class PrefixEqClass(NamedTuple):
    size_eq_class: SizeEqClass
    prefix: bytes


class HashEqClass(NamedTuple):
    prefix_eq_class: PrefixEqClass
    hash: bytes


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
    # After this point it makes no more sense to refine equivalence classes of size 2
    # That's because it's better to just compare the files rather than bother with
    # costly prefix comparisons or hash calculation.
    size_pairs, size_eq_classes = take_out_pairs(get_size_eq_classes(inode_eq_classes))
    logging.info(
        "Found %d pairs of inode classes with same size. Excluding them from further refinement.",
        len(size_pairs),
    )
    logging.info(
        "Remaining inode classes for further refinement: %s",
        members_count(size_eq_classes),
    )
    prefix_pairs, prefix_eq_classes = take_out_pairs(
        get_prefix_eq_classes(inode_eq_classes, size_eq_classes)
    )
    logging.info(
        "Found %d pairs of inode classes with same prefix. Excluding them from further refinement.",
        len(prefix_pairs),
    )
    logging.info(
        "Remaining inode classes for further refinement: %s",
        members_count(prefix_eq_classes),
    )
    hash_eq_classes = get_hash_eq_classes(inode_eq_classes, prefix_eq_classes)
    content_eq_classes = get_content_eq_classes(
        inode_eq_classes,
        itertools.chain(
            hash_eq_classes.values(), size_pairs.values(), prefix_pairs.values()
        ),
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

    non_trivial_size_eq_classes = exclude_unique(size_eq_classes)

    logging.info(
        "Excluded by size: %d (number of inode classes excluded from deduplication because they have unique file sizes)",
        len(size_eq_classes) - len(non_trivial_size_eq_classes),
    )

    return non_trivial_size_eq_classes


RefinedEqClassId = TypeVar("RefinedEqClassId")


def members_count(eq_classes: Dict[EqClassId, Set[EqClassMember]]) -> int:
    return sum(len(eq_class_members) for eq_class_members in eq_classes.values())


def refine_eq_classes(
    eq_classes: Dict[EqClassId, Set[EqClassMember]],
    refine: Callable[[EqClassId, EqClassMember, ProgressCounter], RefinedEqClassId],
) -> Dict[RefinedEqClassId, Set[EqClassMember]]:
    refined_eq_classes: Dict[RefinedEqClassId, Set[EqClassMember]] = {}
    progress_counter = ProgressCounter(remaining=members_count(eq_classes))
    for eq_class_id, eq_class_members in eq_classes.items():
        for eq_class_member in eq_class_members:
            progress_counter.actual += 1
            refined_eq_class_id = refine(eq_class_id, eq_class_member, progress_counter)
            refined_eq_class_members = refined_eq_classes.get(refined_eq_class_id)
            if refined_eq_class_members is None:
                refined_eq_class_members = set()
                refined_eq_classes[refined_eq_class_id] = refined_eq_class_members
            refined_eq_class_members.add(eq_class_member)
    return refined_eq_classes


def exclude_unique(
    eq_classes: Dict[EqClassId, Set[EqClassMember]],
) -> Dict[EqClassId, Set[EqClassMember]]:
    return {
        eq_class_id: eq_class_members
        for eq_class_id, eq_class_members in eq_classes.items()
        if len(eq_class_members) > 1
    }


def take_out_pairs(
    eq_classes: Dict[EqClassId, Set[EqClassMember]],
) -> Tuple[Dict[EqClassId, Set[EqClassMember]], Dict[EqClassId, Set[EqClassMember]]]:
    pairs = {
        eq_class_id: eq_class_members
        for eq_class_id, eq_class_members in eq_classes.items()
        if len(eq_class_members) == 2
    }
    bigger = {
        eq_class_id: eq_class_members
        for eq_class_id, eq_class_members in eq_classes.items()
        if len(eq_class_members) > 2
    }
    return pairs, bigger


def get_prefix_eq_classes(
    inode_eq_classes: Dict[InodeEqClass, Set[FileInfo]],
    size_eq_classes: Dict[SizeEqClass, Set[InodeEqClass]],
) -> Dict[PrefixEqClass, Set[InodeEqClass]]:
    def get_prefix_eq_id(
        eq_class: SizeEqClass, eq_class_member: InodeEqClass, progress: ProgressCounter
    ) -> PrefixEqClass:
        file_info = representative(inode_eq_classes[eq_class_member])
        logging.debug(
            "Comparing file prefix %d out of %d. File size %d. File: '%s'.",
            progress.actual,
            progress.remaining,
            eq_class.size,
            str(file_info.path),
        )
        with file_info.path.open("rb") as file_handle:
            prefix_bytes = file_handle.read(min(eq_class.size, 64))
        return PrefixEqClass(eq_class, prefix_bytes)

    prefix_eq_classes = refine_eq_classes(size_eq_classes, refine=get_prefix_eq_id)
    non_unique_prefix_eq_classes = exclude_unique(prefix_eq_classes)

    logging.info(
        "Excluded by prefix: %d (number of inode classes excluded from deduplication because they start with a unique string of bytes)",
        len(prefix_eq_classes) - len(non_unique_prefix_eq_classes),
    )

    return non_unique_prefix_eq_classes


def get_hash_eq_classes(
    inode_eq_classes: Dict[InodeEqClass, Set[FileInfo]],
    size_eq_classes: Dict[PrefixEqClass, Set[InodeEqClass]],
) -> Dict[HashEqClass, Set[InodeEqClass]]:
    def get_hash_eq_id(
        eq_class: PrefixEqClass,
        eq_class_member: InodeEqClass,
        progress: ProgressCounter,
    ) -> HashEqClass:
        file_info = representative(inode_eq_classes[eq_class_member])
        logging.debug(
            "Calculating hash %d out of %d. File size %d. File: '%s'.",
            progress.actual,
            progress.remaining,
            eq_class.size_eq_class.size,
            str(file_info.path),
        )
        file_hash = hashlib.md5(file_info.path.read_bytes()).digest()
        return HashEqClass(eq_class, file_hash)

    hash_eq_classes = refine_eq_classes(size_eq_classes, refine=get_hash_eq_id)
    non_unique_hash_eq_classes = exclude_unique(hash_eq_classes)

    logging.info(
        "Excluded by hash: %d (number of inode classes excluded from deduplication because they have unique hashes)",
        len(hash_eq_classes) - len(non_unique_hash_eq_classes),
    )

    return non_unique_hash_eq_classes


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
    content_eq_classes: Iterable[Set[InodeEqClass]],
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
