use pyroscope::{pyroscope::PyroscopeAgentRunning, PyroscopeAgent};
use pyroscope_pprofrs::{pprof_backend, PprofConfig};
use snafu::{ResultExt, Whatever};

use crate::env::var;

pub type Guard = PyroscopeAgent<PyroscopeAgentRunning>;

#[must_use]
pub fn init_pyroscope() -> Result<Option<Guard>, Whatever> {
    let url = var("PYROSCOPE_SERVER_URL")?;
    if url.is_none() {
        return Ok(None);
    }

    let url = url.unwrap();
    println!("found pyroscope url: {}, starting pyroscope agent ...", url);

    let agent = PyroscopeAgent::builder(url, String::from("kiseki"))
        .backend(pprof_backend(PprofConfig::new().sample_rate(100)))
        .build()
        .with_whatever_context(|e| "failed to setup pyroscope agent")?;

    let agent_running = agent
        .start()
        .with_whatever_context(|e| "failed to start pyroscope agent")?;

    Ok(Some(agent_running))
}
