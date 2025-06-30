use anyhow::{bail, Result};
use bincode;
use chrono::Utc;
use clap::Args;
use serde::Serialize;
use std::{
    collections::VecDeque,
    fs,
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

use crate::data::{CollectData, CollectorParams};
use crate::record::{
    collect_static_data, prepare_data_collectors, record as run_record, Record,
};
use crate::trigger_parser::parse_cpu_trigger;
use crate::utils::DataMetrics;
use crate::{InitParams, PERFORMANCE_DATA};

// ——— bring in each Raw collector ———
use crate::data::cpu_utilization::{get_aggregate_data, CpuData, CpuUtilizationRaw};
use crate::data::diskstats::DiskstatsRaw;
use crate::data::interrupts::InterruptDataRaw;
use crate::data::meminfodata::MeminfoDataRaw;
use crate::data::netstat::NetstatRaw;
use crate::data::processes::ProcessesRaw;
use crate::data::sysctldata::SysctlData;
use crate::data::vmstat::VmstatRaw;

#[derive(Args, Debug)]
pub struct MonitorArgs {
    /// Trigger when CPU utilization exceeds this percentage
    #[clap(long)]
    pub cpu_usage: f32,

    /// Seconds of history to keep pre-trigger
    #[clap(long)]
    pub period: u64,

    /// Sampling interval in seconds
    #[clap(long, default_value_t = 1)]
    pub interval: u64,

    /// Seconds to record post-trigger
    #[clap(long)]
    pub post: u64,

    /// Cooldown period (seconds) before re-arming
    #[clap(long)]
    pub cooldown: u64,

    /// Base output directory for run data
    #[clap(long, value_parser)]
    pub output: PathBuf,
}

/// Main function to handle trigger-based monitoring from aperf record
/// Simplified version with just the essential optimization: prevent duplicate initialization
pub fn monitor_with_triggers(
    record: &Record,
    trigger_expr: &str,
    tmp_dir: &Path,
    runlog: &Path,
) -> Result<()> {
    // Parse the CPU trigger expression (simple for POC)
    let cpu_trigger = parse_cpu_trigger(trigger_expr)?;
    println!("Parsed CPU trigger: threshold = {}%", cpu_trigger.threshold);

    // Validate required parameters
    if record.interval == 0 || record.period == 0 {
        bail!("`interval` and `period` must both be > 0");
    }

    // Set up output directory
    let output_dir = match &record.output {
        Some(dir) => dir.clone(),
        None => {
            let now = chrono::Utc::now();
            PathBuf::from(format!("aperf_monitor_{}", now.format("%Y%m%d_%H%M%S")))
        }
    };

    // ESSENTIAL OPTIMIZATION: One-time APerf setup to prevent duplicate initialization
    let mut prep_params = InitParams::new(String::new());
    prep_params.period = record.period;
    prep_params.interval = record.interval;
    prep_params.tmp_dir = tmp_dir.to_path_buf();
    prep_params.runlog = runlog.to_path_buf();
    PERFORMANCE_DATA.lock().unwrap().set_params(prep_params);
    PERFORMANCE_DATA.lock().unwrap().init_collectors()?;
    prepare_data_collectors()?;
    collect_static_data()?; // Only called once here

    // Set up circular buffers (keep separate as requested)
    let capacity = (record.period / record.interval) as usize;
    let mut buf_cpu = VecDeque::with_capacity(capacity);
    let mut buf_disk = VecDeque::with_capacity(capacity);
    let mut buf_vm = VecDeque::with_capacity(capacity);
    let mut buf_mem = VecDeque::with_capacity(capacity);
    let mut buf_intr = VecDeque::with_capacity(capacity);
    let mut buf_net = VecDeque::with_capacity(capacity);
    let mut buf_proc = VecDeque::with_capacity(capacity);
    let mut buf_sysctl = VecDeque::with_capacity(capacity);

    // Initialize collectors (keep original approach)
    let mut cpu_raw = CpuUtilizationRaw::new();
    let mut disk_raw = DiskstatsRaw::new();
    let mut vm_raw = VmstatRaw::new();
    let mut mem_raw = MeminfoDataRaw::new();
    let mut intr_raw = InterruptDataRaw::new();
    let mut net_raw = NetstatRaw::new();
    let mut proc_raw = ProcessesRaw::new();
    let mut sysctl_raw = SysctlData::new();

    let params = CollectorParams::new();
    
    // Initial data collection
    cpu_raw.collect_data(&params)?;
    disk_raw.collect_data(&params)?;
    vm_raw.collect_data(&params)?;
    mem_raw.collect_data(&params)?;
    intr_raw.collect_data(&params)?;
    net_raw.collect_data(&params)?;
    proc_raw.collect_data(&params)?;
    sysctl_raw.collect_data(&params)?;

    let mut prev_cpu = cpu_raw.clone();
    let mut trigger_count = 0;
    let mut consecutive_triggers = 0;

    println!("Starting CPU monitoring mode with threshold: {}%", cpu_trigger.threshold);
    println!("Trigger times required: {}", record.trigger_times);
    println!("Max trigger count: {}", record.trigger_count);
    println!("Cooldown period: {} seconds", record.cooldown);

    // Main monitoring loop (keep original logic)
    loop {
        thread::sleep(Duration::from_secs(record.interval));

        // Collect and buffer data (keep separate buffers as requested)
        serialize_and_buffer(&mut cpu_raw, &mut buf_cpu, &params, capacity)?;
        serialize_and_buffer(&mut disk_raw, &mut buf_disk, &params, capacity)?;
        serialize_and_buffer(&mut vm_raw, &mut buf_vm, &params, capacity)?;
        serialize_and_buffer(&mut mem_raw, &mut buf_mem, &params, capacity)?;
        serialize_and_buffer(&mut intr_raw, &mut buf_intr, &params, capacity)?;
        serialize_and_buffer(&mut net_raw, &mut buf_net, &params, capacity)?;
        serialize_and_buffer(&mut proc_raw, &mut buf_proc, &params, capacity)?;
        serialize_and_buffer(&mut sysctl_raw, &mut buf_sysctl, &params, capacity)?;

        // Calculate current CPU usage (keep original APerf logic as requested)
        let cpu_pct = calculate_cpu_usage(&prev_cpu, &cpu_raw)?;
        println!(
            "[{}] Current CPU utilization: {:.2}%",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
            cpu_pct
        );

        // Check if CPU trigger condition is met
        if cpu_pct > cpu_trigger.threshold as f32 {
            consecutive_triggers += 1;
            println!(
                "[{}] CPU trigger condition met: {:.2}% > {:.2}% ({}/{})",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                cpu_pct,
                cpu_trigger.threshold,
                consecutive_triggers,
                record.trigger_times
            );

            if consecutive_triggers >= record.trigger_times {
                trigger_count += 1;
                println!("Triggering recording session {} of {}", trigger_count, record.trigger_count);

                // Create timestamped run directory
                let ts = Utc::now().format("%Y%m%dT%H%M%S").to_string();
                let run_dir = output_dir.join(&ts);
                fs::create_dir_all(&run_dir)?;

                // Dump pre-trigger buffers (keep separate files as requested)
                dump_buffers_to_disk(&run_dir, &ts, &buf_cpu, &buf_disk, &buf_vm, &buf_mem, 
                                   &buf_intr, &buf_net, &buf_proc, &buf_sysctl)?;

                // ESSENTIAL OPTIMIZATION: Run post-trigger recording without re-initialization
                run_record(
                    &Record {
                        run_name: Some(ts.clone()),
                        interval: record.interval,
                        period: record.period,
                        profile: record.profile,
                        perf_frequency: record.perf_frequency,
                        profile_java: record.profile_java.clone(),
                        pmu_config: record.pmu_config.clone(),
                        trigger_metrics: None, // Disable trigger mode for post-recording
                        trigger_times: record.trigger_times,
                        trigger_count: record.trigger_count,
                        cooldown: record.cooldown,
                        output: record.output.clone(),
                        skip_prep: true, // OPTIMIZATION: Skip prep since already initialized
                    },
                    tmp_dir,
                    runlog,
                )?;

                // Move post-trigger data
                let _ = fs::rename(&ts, run_dir.join("post"));

                // Clear buffers and reset counters
                clear_all_buffers(&mut buf_cpu, &mut buf_disk, &mut buf_vm, &mut buf_mem,
                                &mut buf_intr, &mut buf_net, &mut buf_proc, &mut buf_sysctl);
                consecutive_triggers = 0;

                // Check if we've reached max trigger count
                if trigger_count >= record.trigger_count {
                    println!("Reached maximum trigger count ({}). Exiting.", record.trigger_count);
                    break;
                }

                // Cooldown period
                println!("Entering cooldown period for {} seconds", record.cooldown);
                thread::sleep(Duration::from_secs(record.cooldown));
            }
        } else {
            consecutive_triggers = 0;
        }

        // Update previous CPU snapshot for next iteration
        prev_cpu = cpu_raw.clone();
    }

    Ok(())
}

/// Compute busy‐percent from two `CpuUtilizationRaw` snapshots.
fn calculate_cpu_usage(prev: &CpuUtilizationRaw, curr: &CpuUtilizationRaw) -> Result<f32> {
    let mut metrics = DataMetrics::new(String::new());
    let prev_data = crate::data::Data::CpuUtilizationRaw(prev.clone());
    let curr_data = crate::data::Data::CpuUtilizationRaw(curr.clone());

    let proc_prev = crate::data::cpu_utilization::process_gathered_raw_data(prev_data)?;
    let proc_curr = crate::data::cpu_utilization::process_gathered_raw_data(curr_data)?;

    let total_prev = if let crate::data::ProcessedData::CpuUtilization(u) = proc_prev {
        u.total
    } else {
        return Err(anyhow::anyhow!("Expected CpuUtilization data"));
    };
    let total_curr = if let crate::data::ProcessedData::CpuUtilization(u) = proc_curr {
        u.total
    } else {
        return Err(anyhow::anyhow!("Expected CpuUtilization data"));
    };

    let agg_json = get_aggregate_data(vec![total_prev, total_curr], &mut metrics)?;
    let data: Vec<CpuData> = serde_json::from_str(&agg_json)?;
    Ok(100.0 - data.last().map(|d| d.values.idle as f32).unwrap_or(0.0))
}

/// Generic helper: collect, serialize, buffer one Raw collector
fn serialize_and_buffer<R: CollectData + Serialize>(
    raw: &mut R,
    buf: &mut VecDeque<Vec<u8>>,
    params: &CollectorParams,
    capacity: usize,
) -> Result<()> {
    raw.collect_data(params)?;
    let mut blob = Vec::new();
    bincode::serialize_into(&mut blob, raw)?;
    buf.push_back(blob);
    if buf.len() > capacity {
        buf.pop_front();
    }
    Ok(())
}

/// Dump all buffers to disk
fn dump_buffers_to_disk(
    run_dir: &Path,
    ts: &str,
    buf_cpu: &VecDeque<Vec<u8>>,
    buf_disk: &VecDeque<Vec<u8>>,
    buf_vm: &VecDeque<Vec<u8>>,
    buf_mem: &VecDeque<Vec<u8>>,
    buf_intr: &VecDeque<Vec<u8>>,
    buf_net: &VecDeque<Vec<u8>>,
    buf_proc: &VecDeque<Vec<u8>>,
    buf_sysctl: &VecDeque<Vec<u8>>,
) -> Result<()> {
    dump_buffer_to_file(buf_cpu, &run_dir.join(format!("cpu_utilization_{}.bin", ts)))?;
    dump_buffer_to_file(buf_disk, &run_dir.join(format!("disk_stats_{}.bin", ts)))?;
    dump_buffer_to_file(buf_vm, &run_dir.join(format!("vmstat_{}.bin", ts)))?;
    dump_buffer_to_file(buf_mem, &run_dir.join(format!("meminfo_{}.bin", ts)))?;
    dump_buffer_to_file(buf_intr, &run_dir.join(format!("interrupts_{}.bin", ts)))?;
    dump_buffer_to_file(buf_net, &run_dir.join(format!("netstat_{}.bin", ts)))?;
    dump_buffer_to_file(buf_proc, &run_dir.join(format!("processes_{}.bin", ts)))?;
    dump_buffer_to_file(buf_sysctl, &run_dir.join(format!("sysctl_{}.bin", ts)))?;
    Ok(())
}

/// Dump a single buffer to a file
fn dump_buffer_to_file(buffer: &VecDeque<Vec<u8>>, file_path: &Path) -> Result<()> {
    let mut file = File::create(file_path)?;
    for blob in buffer {
        file.write_all(blob)?;
    }
    Ok(())
}

/// Clear all buffers
fn clear_all_buffers(
    buf_cpu: &mut VecDeque<Vec<u8>>,
    buf_disk: &mut VecDeque<Vec<u8>>,
    buf_vm: &mut VecDeque<Vec<u8>>,
    buf_mem: &mut VecDeque<Vec<u8>>,
    buf_intr: &mut VecDeque<Vec<u8>>,
    buf_net: &mut VecDeque<Vec<u8>>,
    buf_proc: &mut VecDeque<Vec<u8>>,
    buf_sysctl: &mut VecDeque<Vec<u8>>,
) {
    buf_cpu.clear();
    buf_disk.clear();
    buf_vm.clear();
    buf_mem.clear();
    buf_intr.clear();
    buf_net.clear();
    buf_proc.clear();
    buf_sysctl.clear();
}

// Keep the old monitor function for backward compatibility (if needed)
pub fn monitor(args: &MonitorArgs, tmp_dir: &Path, runlog: &Path) -> Result<()> {
    // Convert MonitorArgs to Record format and call the new function
    let record = Record {
        run_name: None,
        interval: args.interval,
        period: args.period,
        profile: false,
        perf_frequency: 99,
        profile_java: None,
        pmu_config: None,
        trigger_metrics: Some(format!("cpu > {}", args.cpu_usage)),
        trigger_times: 1,
        trigger_count: 10,
        cooldown: args.cooldown,
        output: Some(args.output.clone()),
        skip_prep: false,
    };

    monitor_with_triggers(&record, &format!("cpu > {}", args.cpu_usage), tmp_dir, runlog)
}
