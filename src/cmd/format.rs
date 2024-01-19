use clap::Args;
use snafu::Whatever;

const FORMAT_OPTIONS_HEADER: &str = "Format options";

#[derive(Debug, Clone, Args)]
#[command(args_conflicts_with_subcommands = true)]
#[command(flatten_help = true)]
#[command(long_about = r"

Create a new KisekiFS volume. Here META_ADDRESS is used to set up the
metadata engine location, the SCHEME is used to specific the meta sto
engine, and NAME is the prefix of all objects in data storage.
")]
pub struct FormatArgs {
    #[arg(
        value_name = "FILE_SYSTEM_NAME",
        help = r"Your file system name, like 'demo-fs'."
    )]
    pub name: String,

    #[arg(
    long,
    help = "Specify the address of the meta store",
    help_heading = FORMAT_OPTIONS_HEADER,
    default_value = "/tmp/kiseki-meta",
    )]
    pub meta_address: String,

    #[arg(
    long,
    help = "Specify the scheme of the meta store",
    help_heading = FORMAT_OPTIONS_HEADER,
    default_value_t = opendal::Scheme::Memory.to_string(),
    )]
    pub scheme: String, // FIXME
}
impl FormatArgs {
    pub fn run(&self) -> Result<(), Whatever> {
        todo!()
    }
}
