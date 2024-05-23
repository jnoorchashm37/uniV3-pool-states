use clap::{ArgAction, Args, Parser};

use tracing::{level_filters::LevelFilter, Level};
use tracing_subscriber::filter::Directive;

#[derive(Debug, Parser)]
#[command(about = "Uniswap V3 Pool Calls", long_about = None)]
pub struct CliCmd {
    /// calls `slot0()` on the UniV3 contract after each transaction that altered the pool's state
    #[arg(short = 'l', long, default_value = "false")]
    pub slot0: bool,

    /// calls `tick()` on the UniV3 contract for each initialized tick after each transaction that altered the pool's state
    #[arg(short, long, default_value = "false")]
    pub tick_info: bool,

    /// default is the block of the creation of the first uniV3 pool
    #[arg(short, long)]
    pub start_block: Option<u64>,
    /// defaults is the current chain tip
    #[arg(short, long)]
    pub end_block: Option<u64>,

    #[clap(flatten)]
    pub verbosity: Verbosity,
}

/// The verbosity settings for the cli.
#[derive(Debug, Copy, Clone, Args)]
#[command(next_help_heading = "Display")]
pub struct Verbosity {
    /// Set the minimum log level.
    ///
    /// -v      Errors
    /// -vv     Warnings
    /// -vvv    Info
    /// -vvvv   Debug
    /// -vvvvv  Traces (warning: very verbose!)
    #[clap(short, long, action = ArgAction::Count, global = true, default_value_t = 3, verbatim_doc_comment, help_heading = "Display")]
    verbosity: u8,

    /// Silence all log output.
    #[clap(
        long,
        alias = "silent",
        short = 'q',
        global = true,
        help_heading = "Display"
    )]
    quiet: bool,
}

impl Verbosity {
    /// Get the corresponding [Directive] for the given verbosity, or none if
    /// the verbosity corresponds to silent.
    pub fn directive(&self) -> Directive {
        if self.quiet {
            LevelFilter::OFF.into()
        } else {
            let level = match self.verbosity - 1 {
                0 => Level::ERROR,
                1 => Level::WARN,
                2 => Level::INFO,
                3 => Level::DEBUG,
                _ => Level::TRACE,
            };

            format!("uniV3={level}").parse().unwrap()
        }
    }
}
