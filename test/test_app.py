import pytest

from hardlink_dedup import app


def test_dedup_empty_dir(tmp_path):
    """an empty directory stays empty after deduplication"""
    dir_contents_before = list(tmp_path.iterdir())
    app.dedup([tmp_path])
    dir_contents_after = list(tmp_path.iterdir())
    assert dir_contents_before == dir_contents_after


def test_dedup_different_files(foo_file, bar_file):
    """different files should stay different and not deleted"""
    app.dedup([foo_file.parent, bar_file.parent])
    assert foo_file.read_text() != bar_file.read_text()
    assert list(foo_file.parent.iterdir()) == [foo_file]
    assert list(bar_file.parent.iterdir()) == [bar_file]


def test_dedup_symlink(foo_file, symlink_to_foo):
    """symlinks should stay symlinks"""
    app.dedup([foo_file.parent, symlink_to_foo.parent])
    assert symlink_to_foo.is_symlink()


def test_dedup_files_with_same_contents(foo_file, tmp_path):
    """two different files with the same content should be hardlinked"""
    other_foo_file = create_file(tmp_path / "otherFooDir")
    app.dedup([foo_file.parent, other_foo_file.parent])
    assert other_foo_file.samefile(foo_file)


def test_dedup_3_files_with_same_contents(foo_file, tmp_path):
    """three different files with the same content should be hardlinked"""
    other_foo_file = create_file(tmp_path / "otherFooDir")
    third_foo_file = create_file(tmp_path / "thirdFooDir")
    app.dedup([tmp_path])
    assert other_foo_file.samefile(foo_file)
    assert third_foo_file.samefile(foo_file)


def test_dedup_files_newlines_in_names(tmp_path):
    """dedup must support newlines in file names"""
    newline_file1 = create_file(tmp_path / "Dir\n1", file_name="newline\nname1")
    newline_file2 = create_file(tmp_path / "Dir\r\n2", file_name="newline\r\nname2")
    app.dedup([tmp_path])
    assert newline_file1.samefile(newline_file2)


def test_get_file_infos_direct_children(foo_file, bar_file):
    """file information is retrieved for direct children of given directories"""
    assert set(app.get_file_infos([foo_file.parent, bar_file.parent])) == {
        app.FileInfo(
            foo_file, app.Inode(foo_file.stat().st_dev, foo_file.stat().st_ino, 6)
        ),
        app.FileInfo(
            bar_file, app.Inode(bar_file.stat().st_dev, bar_file.stat().st_ino, 9)
        ),
    }


def test_get_file_infos_no_symlinks(symlink_to_foo):
    """symlinks are not included in deduplication efforts"""
    assert not list(app.get_file_infos([symlink_to_foo.parent]))


@pytest.fixture(name="foo_file")
def foo_file_fixture(tmp_path):
    return create_file(tmp_path / "dirFoo")


@pytest.fixture(name="symlink_to_foo")
def symlink_to_foo_fixture(tmp_path, foo_file):
    symlink_to_foo = tmp_path / "symlinkDir" / "foo_symlink"
    symlink_to_foo.parent.mkdir()
    symlink_to_foo.symlink_to(foo_file)
    return symlink_to_foo


@pytest.fixture(name="bar_file")
def bar_file_fixture(tmp_path):
    bar_file = tmp_path / "dirBar" / "bar"
    bar_file.parent.mkdir()
    bar_file.write_text("hello bar")
    return bar_file


def create_file(parent_dir, file_name="foo"):
    foo_file = parent_dir / file_name
    foo_file.parent.mkdir()
    foo_file.write_text("hi foo")
    return foo_file
