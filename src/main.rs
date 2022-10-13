use clap::Parser;
use hardlink_dedup::dedup;
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Parser, Debug)]
#[command(about = "Incrementally hardlinks files with the same contents.")]
struct Args {
    /// Don't actually hardlink any files.
    #[arg(long, short = 'n', default_value_t = false)]
    dry_run: bool,

    /// Don't trust the sha-256 hashing algorithm and always check that files are indeed bit-for-bit equal.
    /// This option is slower.
    #[arg(long, short = 'p', default_value_t = false)]
    paranoid: bool,

    /// Paths (directories or files) to deduplicate. Directories will be recursively traversed. Softlinks won't be followed.
    /// If no paths are specified nothing will be deduped.
    paths: Vec<PathBuf>,
}

fn main() -> ExitCode {
    let args = Args::parse();
    env_logger::init();
    dedup(&args.paths, args.dry_run, args.paranoid);
    ExitCode::SUCCESS
}
