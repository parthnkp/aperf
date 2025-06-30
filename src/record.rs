use crate::{data, InitParams, PERFORMANCE_DATA};
use anyhow::anyhow;
use anyhow::Result;
use clap::Args;
use log::{debug, error, info};
use std::path::{Path, PathBuf};
//use std::time::{self, Instant};

#[derive(Args, Debug)]
pub struct Record {
    /// Name of the run.
    #[clap(short, long, value_parser)]
    pub run_name: Option<String>,

    /// Interval (in seconds) at which performance data is to be collected.
    #[clap(short, long, value_parser, default_value_t = 1)]
    pub interval: u64,

    /// Time (in seconds) for which the performance data is to be collected.
    #[clap(short, long, value_parser, default_value_t = 10)]
    pub period: u64,

    /// Gather profiling data using 'perf' binary.
    #[clap(long, value_parser)]
    pub profile: bool,

    /// Frequency for perf profiling (Hz).
    #[clap(short = 'F', long, value_parser, default_value_t = 99)]
    pub perf_frequency: u32,

    /// Profile JVMs using async-profiler. Specify args using comma separated values. Profiles all JVMs if no args are provided.
    #[clap(long, value_parser, default_missing_value = Some("jps"), value_names = &["PID/Name>,<PID/Name>,...,<PID/Name"], num_args = 0..=1)]
    pub profile_java: Option<String>,

    /// Custom PMU config file to use.
    #[clap(long, value_parser)]
    pub pmu_config: Option<String>,

    /// Enable monitoring mode with trigger conditions (e.g., 'cpu > 80', 'cpu > 80 && mem > 50')
    #[clap(long, value_parser)]
    pub trigger_metrics: Option<String>,

    /// Number of consecutive positive conditions needed to start recording
    #[clap(long, value_parser, default_value_t = 1)]
    pub trigger_times: u32,

    /// Maximum number of recording sessions before complete exit
    #[clap(long, value_parser, default_value_t = 10)]
    pub trigger_count: u32,

    /// Cooldown period (seconds) before re-arming after trigger
    #[clap(long, value_parser, default_value_t = 1200)] // 20 minutes default
    pub cooldown: u64,

    /// Base output directory for triggered runs
    #[clap(long, value_parser)]
    pub output: Option<PathBuf>,

    #[clap(skip)]
    pub skip_prep: bool,
}

pub fn prepare_data_collectors() -> Result<()> {
    info!("Preparing data collectors...");
    PERFORMANCE_DATA.lock().unwrap().prepare_data_collectors()?;
    Ok(())
}

fn start_collection_serial() -> Result<()> {
    info!("Collecting data...");
    PERFORMANCE_DATA.lock().unwrap().collect_data_serial()?;
    Ok(())
}

pub fn collect_static_data() -> Result<()> {
    debug!("Collecting static data...");
    PERFORMANCE_DATA.lock().unwrap().collect_static_data()?;
    Ok(())
}

pub fn record(record: &Record, tmp_dir: &Path, runlog: &Path) -> Result<()> {
    // Check if this is a trigger-based monitoring mode
    if let Some(trigger_expr) = &record.trigger_metrics {
        return crate::monitor::monitor_with_triggers(record, trigger_expr, tmp_dir, runlog);
    }

    // Original record functionality for non-trigger mode
    let mut run_name = String::new();
    if record.period == 0 {
        error!("Collection period cannot be 0.");
        return Err(anyhow!("Cannot start recording with the given parameters."));
    }
    if record.interval == 0 {
        error!("Collection interval cannot be 0.");
        return Err(anyhow!("Cannot start recording with the given parameters."));
    }
    // Check if interval > period , if so give error user and exit.
    if record.interval >= record.period {
        error!("The overall recording period of {period} seconds needs to be longer than the interval of {interval} seconds.\
                Please increase the overall recording period or decrease the interval.", interval = record.interval, period =record.period);
        return Err(anyhow!("Cannot start recording with the given parameters."));
    }
    match &record.run_name {
        Some(r) => run_name = r.clone(),
        None => {}
    }
    let mut params = InitParams::new(run_name);
    params.period = record.period;
    params.interval = record.interval;
    params.tmp_dir = tmp_dir.to_path_buf();
    params.runlog = runlog.to_path_buf();
    if let Some(p) = &record.pmu_config {
        params.pmu_config = Some(PathBuf::from(p));
    }

    match &record.profile_java {
        Some(j) => {
            params.profile.insert(
                String::from(data::java_profile::JAVA_PROFILE_FILE_NAME),
                j.clone(),
            );
        }
        None => {}
    }
    if record.profile {
        params.profile.insert(
            String::from(data::perf_profile::PERF_PROFILE_FILE_NAME),
            String::new(),
        );
        params.profile.insert(
            String::from(data::flamegraphs::FLAMEGRAPHS_FILE_NAME),
            String::new(),
        );
        params.perf_frequency = record.perf_frequency;
    }
    //let start = Instant::now();
    PERFORMANCE_DATA.lock().unwrap().set_params(params);
    PERFORMANCE_DATA.lock().unwrap().init_collectors()?;
    //info!("Initializing collectors took {:?}", start.elapsed());
    info!("Starting Data collection...");
    if !record.skip_prep {
        prepare_data_collectors()?;
    }
    //let start = Instant::now();
    //prepare_data_collectors()?;
    //info!("Preparing data collectors took {:?}", start.elapsed());
    collect_static_data()?;
    start_collection_serial()?;
    info!("Data collection complete.");
    PERFORMANCE_DATA.lock().unwrap().end()?;

    Ok(())
}
