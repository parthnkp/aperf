use crate::data::cpu_utilization::{get_aggregate_data, CpuData, CpuUtilization};
use crate::data::{
    cpu_utilization::CpuUtilizationRaw, CollectData, CollectorParams, Data, ProcessedData,
};
use crate::record::{record, Record};
use crate::utils::DataMetrics;
use anyhow::{anyhow, Result};
use clap::Args;
use log::{debug, info};
use nix::sys::signal;
use serde_json;
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Args, Debug)]
pub struct Monitor {
    /// Name of the run when triggered.
    #[clap(short, long, value_parser)]
    pub run_name: Option<String>,

    /// Interval (in seconds) at which metrics are checked.
    #[clap(short, long, value_parser, default_value_t = 1)]
    pub check_interval: u64,

    /// CPU utilization threshold (percentage) to trigger recording.
    #[clap(long, value_parser)]
    pub cpu_threshold: Option<f32>,

    /// Interval (in seconds) at which performance data is collected when triggered.
    #[clap(long, value_parser, default_value_t = 1)]
    pub record_interval: u64,

    /// Time (in seconds) for which the performance data is collected when triggered.
    #[clap(long, value_parser, default_value_t = 30)]
    pub record_period: u64,

    /// Gather profiling data using 'perf' binary when triggered.
    #[clap(long, value_parser)]
    pub profile: bool,

    /// Profile JVMs using async-profiler when triggered.
    #[clap(long, value_parser, default_missing_value = Some("jps"), value_names = &["PID/Name>,<PID/Name>,...,<PID/Name"], num_args = 0..=1)]
    pub profile_java: Option<String>,

    /// Custom PMU config file to use when triggered.
    #[clap(long, value_parser)]
    pub pmu_config: Option<String>,

    /// Cooldown period (in seconds) after a trigger before monitoring again.
    #[clap(long, value_parser, default_value_t = 300)]
    pub cooldown: u64,

    /// Maximum number of iterations to run (for testing)
    #[clap(long, value_parser)]
    pub max_iterations: Option<u32>,
}

pub fn monitor(monitor: &Monitor, tmp_dir: &Path, runlog: &Path) -> Result<()> {
    // Check if CPU threshold is specified
    if monitor.cpu_threshold.is_none() {
        return Err(anyhow!(
            "CPU threshold must be specified using --cpu_threshold"
        ));
    }

    let cpu_threshold = monitor.cpu_threshold.unwrap();
    info!(
        "Starting CPU utilization monitoring with threshold: {}%",
        cpu_threshold
    );
    info!(
        "Will run for {} iterations",
        monitor.max_iterations.unwrap_or(u32::MAX)
    );

    let mut last_trigger_time = Instant::now() - Duration::from_secs(monitor.cooldown);

    // Create a minimal CollectorParams
    let collector_params = CollectorParams {
        collection_time: 0,
        elapsed_time: 0,
        data_file_path: PathBuf::new(),
        data_dir: PathBuf::new(),
        run_name: String::new(),
        profile: HashMap::new(),
        tmp_dir: tmp_dir.to_path_buf(),
        signal: signal::SIGTERM,
        runlog: runlog.to_path_buf(),
        pmu_config: None,
    };

    // Create CPU utilization collectors
    let mut cpu_raw_prev = CpuUtilizationRaw::new();
    let mut cpu_raw_curr = CpuUtilizationRaw::new();

    // Collect initial data
    cpu_raw_prev.collect_data(&collector_params)?;

    // Main monitoring loop
    let mut iterations = 0;
    let max_iterations = monitor.max_iterations.unwrap_or(u32::MAX);

    while iterations < max_iterations {
        iterations += 1;

        // Sleep for the check interval
        thread::sleep(Duration::from_secs(monitor.check_interval));

        // Collect current CPU data
        cpu_raw_curr.collect_data(&collector_params)?;

        // Process the raw data to get CPU utilization
        let raw_data_prev = Data::CpuUtilizationRaw(cpu_raw_prev.clone());
        let raw_data_curr = Data::CpuUtilizationRaw(cpu_raw_curr.clone());

        let mut cpu_util = ProcessedData::CpuUtilization(CpuUtilization::new());
        let processed_data_prev = cpu_util.process_raw_data(raw_data_prev)?;
        let processed_data_curr = cpu_util.process_raw_data(raw_data_curr)?;

        // Extract CPU utilization data
        let cpu_util_prev = match processed_data_prev {
            ProcessedData::CpuUtilization(util) => util,
            _ => return Err(anyhow!("Invalid processed data type")),
        };

        let cpu_util_curr = match processed_data_curr {
            ProcessedData::CpuUtilization(util) => util,
            _ => return Err(anyhow!("Invalid processed data type")),
        };

        // Calculate CPU usage using the existing functionality
        let mut metrics = DataMetrics::new(String::new());
        let cpu_data = vec![cpu_util_prev.total, cpu_util_curr.total];
        let json_result = get_aggregate_data(cpu_data, &mut metrics)?;

        // Parse the result to get CPU usage
        let cpu_values: Vec<CpuData> = serde_json::from_str(&json_result)?;

        // After parsing the JSON result
        //info!("CPU values: {:?}", cpu_values);
        //info!("Idle percentage: {:.2}%", cpu_values[0].values.idle);

        // The CPU usage is 100 - idle percentage
        let cpu_usage = if !cpu_values.is_empty() {
            let last_index = cpu_values.len() - 1;
            100.0 - cpu_values[last_index].values.idle as f32
        } else {
            0.0
        };

        info!("Current CPU usage: {:.2}%", cpu_usage);

        // Check if threshold is exceeded
        if cpu_usage > cpu_threshold {
            info!(
                "CPU threshold exceeded: {:.2}% > {:.2}%",
                cpu_usage, cpu_threshold
            );

            // Check cooldown period
            let current_time = Instant::now();
            if current_time.duration_since(last_trigger_time).as_secs() > monitor.cooldown {
                // Trigger recording
                trigger_recording(monitor, tmp_dir, runlog, "cpu")?;
                last_trigger_time = current_time;
            } else {
                debug!("In cooldown period, not triggering recording");
            }
        }

        // Update previous collector with current data
        cpu_raw_prev = cpu_raw_curr.clone();
    }

    info!("Monitoring stopped after {} iterations.", iterations);
    Ok(())
}

fn trigger_recording(
    monitor: &Monitor,
    tmp_dir: &Path,
    runlog: &Path,
    trigger_type: &str,
) -> Result<()> {
    info!("Threshold exceeded! Triggering APerf recording...");

    // Create a Record struct with the monitor's parameters
    let mut record_params = Record {
        run_name: monitor.run_name.clone(),
        interval: monitor.record_interval,
        period: monitor.record_period,
        profile: monitor.profile,
        profile_java: monitor.profile_java.clone(),
        pmu_config: monitor.pmu_config.clone(),
    };

    // If no run name was provided, create one with timestamp and trigger type
    if record_params.run_name.is_none() {
        let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S").to_string();
        record_params.run_name = Some(format!("auto_{}_{}", trigger_type, timestamp));
    }

    // Call the record function with our parameters
    info!(
        "Starting recording with run name: {:?}",
        record_params.run_name
    );
    record(&record_params, tmp_dir, runlog)?;

    info!("Recording complete. Resuming monitoring...");
    Ok(())
}
