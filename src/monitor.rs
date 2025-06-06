use crate::data::cpu_utilization::{
    get_aggregate_data, CpuData, CpuUtilization, CpuUtilizationRaw,
};
use crate::data::{CollectData, CollectorParams, Data, ProcessedData};
use crate::record::{record, Record};
use crate::utils::DataMetrics;
use anyhow::{anyhow, Result};
use clap::Args;
use log::{debug, info};
use nix::sys::signal;
use regex::Regex;
use serde_json;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Args, Debug)]
pub struct Monitor {
    /// Name of the run when triggered.
    #[clap(short, long, value_parser)]
    pub run_name: Option<String>,

    /// Interval (in seconds) at which metrics are checked.
    #[clap(long, value_parser, default_value_t = 1)]
    pub trigger_interval: u64,

    /// Expression defining metrics and conditions for triggering.
    #[clap(long, value_parser)]
    pub trigger_metrics: Option<String>,

    /// How many times the condition needs to be positive to trigger recording.
    #[clap(long, value_parser, default_value_t = 3)]
    pub trigger_times: u32,

    /// How many times to record before exiting (1-100).
    #[clap(long, value_parser, default_value_t = 1)]
    pub trigger_count: u32,

    /// How much time to wait after recording before restarting monitoring.
    #[clap(long, value_parser, default_value_t = 60)]
    pub trigger_sleep_time: u64,

    /// Record for the entire duration where conditions are matched.
    #[clap(long, value_parser)]
    pub trigger_record_entirely: bool,

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

    /// Maximum number of iterations to run (for testing)
    #[clap(long, value_parser)]
    pub max_iterations: Option<u32>,

    // Keep the old parameters for backward compatibility
    /// CPU utilization threshold (percentage) to trigger recording (deprecated).
    #[clap(long, value_parser, hide = true)]
    pub cpu_threshold: Option<f32>,

    /// Cooldown period (in seconds) after a trigger before monitoring again (deprecated).
    #[clap(long, value_parser, default_value_t = 300, hide = true)]
    pub cooldown: u64,
}

fn evaluate_expression(expr: &str, metrics: &HashMap<String, f32>) -> Result<bool> {
    
    // handle operator precedence, parentheses, etc.
    let re = Regex::new(r"(\w+)\s*([><]=?|==|!=)\s*(\d+(?:\.\d+)?)")?;

    for cap in re.captures_iter(expr) {
        let metric_name = &cap[1];
        let operator = &cap[2];
        let threshold: f32 = cap[3].parse()?;

        if metric_name == "custom" {
            // Handle custom script
            let script_path = threshold.to_string();
            let output = Command::new("sh").arg("-c").arg(&script_path).output()?;

            let exit_code = output.status.code().unwrap_or(1);
            return Ok(exit_code == 0);
        }

        let metric_value = metrics.get(metric_name).copied().unwrap_or(0.0);

        let result = match operator {
            ">" => metric_value > threshold,
            "<" => metric_value < threshold,
            ">=" => metric_value >= threshold,
            "<=" => metric_value <= threshold,
            "==" => (metric_value - threshold).abs() < std::f32::EPSILON,
            "!=" => (metric_value - threshold).abs() >= std::f32::EPSILON,
            _ => return Err(anyhow!("Unknown operator: {}", operator)),
        };

        if !result {
            return Ok(false);
        }
    }

    Ok(true)
}

pub fn monitor(monitor: &Monitor, tmp_dir: &Path, runlog: &Path) -> Result<()> {
    // Check if trigger metrics is specified
    if monitor.trigger_metrics.is_none() && monitor.cpu_threshold.is_none() {
        return Err(anyhow!(
            "Either --trigger_metrics or --cpu_threshold must be specified"
        ));
    }

    // For backward compatibility
    let trigger_expr = monitor
        .trigger_metrics
        .clone()
        .unwrap_or_else(|| format!("cpu > {}", monitor.cpu_threshold.unwrap_or(80.0)));

    info!(
        "Starting metrics monitoring with expression: {}",
        trigger_expr
    );
    info!(
        "Will run for {} iterations",
        monitor.max_iterations.unwrap_or(u32::MAX)
    );

    let mut last_trigger_time = Instant::now() - Duration::from_secs(monitor.trigger_sleep_time);
    let mut positive_condition_count = 0;
    let mut recordings_done = 0;

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

    // Create collectors for different metrics
    let mut cpu_raw_prev = CpuUtilizationRaw::new();
    // Add other collectors here as needed (memory, network, etc.)

    // Collect initial data
    cpu_raw_prev.collect_data(&collector_params)?;

    // Main monitoring loop
    let mut iterations = 0;
    let max_iterations = monitor.max_iterations.unwrap_or(u32::MAX);

    while iterations < max_iterations && recordings_done < monitor.trigger_count {
        iterations += 1;

        // Sleep for the check interval
        thread::sleep(Duration::from_secs(monitor.trigger_interval));

        // Collect current metrics
        let mut cpu_raw_curr = CpuUtilizationRaw::new();
        cpu_raw_curr.collect_data(&collector_params)?;

        // Process CPU data
        let cpu_usage = calculate_cpu_usage(&cpu_raw_prev, &cpu_raw_curr)?;
        info!("Current CPU usage: {:.2}%", cpu_usage);

        // Collect all metrics into a map
        let mut metrics = HashMap::new();
        metrics.insert("cpu".to_string(), cpu_usage);
        // Add other metrics here as needed

        // Evaluate the trigger expression
        let condition_met = evaluate_expression(&trigger_expr, &metrics)?;

        if condition_met {
            info!("Condition met: {}", trigger_expr);
            positive_condition_count += 1;

            if positive_condition_count >= monitor.trigger_times {
                // Check cooldown period
                let current_time = Instant::now();
                if current_time.duration_since(last_trigger_time).as_secs()
                    > monitor.trigger_sleep_time
                {
                    // Trigger recording
                    trigger_recording(monitor, tmp_dir, runlog, "metrics")?;
                    last_trigger_time = current_time;
                    positive_condition_count = 0;
                    recordings_done += 1;
                } else {
                    debug!("In cooldown period, not triggering recording");
                }
            } else {
                info!(
                    "Condition met {}/{} times",
                    positive_condition_count, monitor.trigger_times
                );
            }
        } else {
            // Reset the counter if condition is not met
            if positive_condition_count > 0 {
                info!("Condition no longer met, resetting counter");
                positive_condition_count = 0;
            }
        }

        // Update previous data
        cpu_raw_prev = cpu_raw_curr;
    }

    if recordings_done >= monitor.trigger_count {
        info!("Completed {} recordings as requested", recordings_done);
    } else {
        info!("Monitoring stopped after {} iterations", iterations);
    }

    Ok(())
}

// Helper function to calculate CPU usage
fn calculate_cpu_usage(prev_raw: &CpuUtilizationRaw, curr_raw: &CpuUtilizationRaw) -> Result<f32> {
    // Process the raw data to get CPU utilization
    let raw_data_prev = Data::CpuUtilizationRaw(prev_raw.clone());
    let raw_data_curr = Data::CpuUtilizationRaw(curr_raw.clone());

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

    // The CPU usage is 100 - idle percentage
    let cpu_usage = if !cpu_values.is_empty() {
        let last_index = cpu_values.len() - 1;
        100.0 - cpu_values[last_index].values.idle as f32
    } else {
        0.0
    };

    Ok(cpu_usage)
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
