# hardlink-dedup

Incrementally deduplicates files through hardlinking.

If it's taking too long, you can interrupt this program in the middle of a run.
If it managed to deduplicate at least some files during an aborted run, then it
will be faster next time you run it.

Currently this crate is not distributed anywhere, so you'll have to pull it and
build it yourself. Stay tuned.