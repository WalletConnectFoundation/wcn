use {
    wcn::AdminApiArgs,
    std::{str::FromStr, time::Duration},
};

#[derive(Debug, clap::Args)]
pub struct Cmd {
    #[clap(flatten)]
    admin_api_args: AdminApiArgs,

    /// Memory profile duration.
    #[clap(short, long)]
    duration: ProfileDuration,

    #[clap(short, long, default_value = "dhat-profile.json")]
    /// Output file for the dhat profile.
    out_file: String,
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error(transparent)]
    Api(#[from] wcn_admin_api::client::Error<wcn_admin_api::MemoryProfileError>),

    #[error("Failed to parse duration: {0}")]
    DurationFormat(humantime::DurationError),

    #[error("Invalid duration")]
    InvalidDuration,

    #[error("Failed to write memory profile: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone, Copy)]
struct ProfileDuration(Duration);

impl FromStr for ProfileDuration {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        humantime::parse_duration(s)
            .map_err(Error::DurationFormat)
            .and_then(|duration| {
                if duration.is_zero() || duration > wcn_admin_api::MEMORY_PROFILE_MAX_DURATION {
                    Err(Error::InvalidDuration)
                } else {
                    Ok(Self(duration))
                }
            })
    }
}

pub async fn exec(cmd: Cmd) -> anyhow::Result<()> {
    use wcn_admin_api::snap;

    let data = cmd
        .admin_api_args
        .new_client()?
        .memory_profile(cmd.duration.0)
        .await
        .map_err(Error::from)?;

    std::io::copy(
        &mut snap::read::FrameDecoder::new(&data.dhat[..]),
        &mut std::fs::File::create(cmd.out_file)?,
    )?;

    Ok(())
}
