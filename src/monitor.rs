use anyhow::{bail, Result};
use chrono::Utc;
use clap::Args;
use std::{
    collections::VecDeque,
    fs,
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};

use crate::data::cpu_utilization::CpuUtilizationRaw;
use crate::data::cpu_utilization::{get_aggregate_data, CpuData};
use crate::data::{CollectData, CollectorParams};
use crate::record::{record as run_record, Record as RecordOpts};
use crate::utils::DataMetrics;
use bincode;

/// `aperf monitor` CLI arguments
#[derive(Args, Debug)]
pub struct MonitorArgs {
    /// Trigger when CPU utilization exceeds this percentage
    #[clap(long)]
    pub threshold: f32,

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

/// Reads two raw snapshots and returns the busy‐percent using the existing pipeline.
fn calculate_cpu_usage(prev: &CpuUtilizationRaw, curr: &CpuUtilizationRaw) -> Result<f32> {
    let mut metrics = DataMetrics::new(String::new());
    let prev_data = crate::data::Data::CpuUtilizationRaw(prev.clone());
    let curr_data = crate::data::Data::CpuUtilizationRaw(curr.clone());

    // Process into ProcessedData
    let proc_prev = crate::data::cpu_utilization::process_gathered_raw_data(prev_data)?;
    let proc_curr = crate::data::cpu_utilization::process_gathered_raw_data(curr_data)?;

    // Extract totals
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

    // Aggregate → Vec<CpuData>
    let agg_json = get_aggregate_data(vec![total_prev, total_curr], &mut metrics)?;
    let data: Vec<CpuData> = serde_json::from_str(&agg_json)?;

    // busy% = 100 - idle%
    let busy = data
        .last()
        .map(|d| 100.0 - d.values.idle as f32)
        .unwrap_or(0.0);
    Ok(busy)
}

pub fn monitor(args: &MonitorArgs, tmp_dir: &Path, runlog: &Path) -> Result<()> {
    // 1) Sanity checks
    if args.interval == 0 || args.period == 0 || args.post == 0 {
        bail!("`interval`, `period`, and `post` must all be > 0");
    }
    if args.interval >= args.period {
        bail!(
            "`period` ({}) must be larger than `interval` ({})",
            args.period,
            args.interval
        );
    }

    // 2) Prepare a VecDeque as a circular buffer for raw blobs
    let capacity = (args.period / args.interval) as usize;
    let mut buf: VecDeque<Vec<u8>> = VecDeque::with_capacity(capacity);

    // 3) Prime the “previous raw” by taking one snapshot
    let mut prev_raw = CpuUtilizationRaw::new();
    let params = CollectorParams::new();
    prev_raw.collect_data(&params)?;

    loop {
        // 4) Sleep until next tick
        thread::sleep(Duration::from_secs(args.interval));

        // 5) Take a fresh raw snapshot and serialize to bytes
        let mut curr_raw = CpuUtilizationRaw::new();
        curr_raw.collect_data(&params)?;
        let mut blob = Vec::new();
        bincode::serialize_into(&mut blob, &curr_raw)?;

        // 6) Push raw blob into buffer, popping the oldest if full
        buf.push_back(blob);
        if buf.len() > capacity {
            buf.pop_front();
        }

        // 7) Only check once buffer is full
        if buf.len() < capacity {
            prev_raw = curr_raw;
            continue;
        }

        // 8) Calculate CPU% using last two raws
        let cpu_pct = calculate_cpu_usage(&prev_raw, &curr_raw)?;
        prev_raw = curr_raw;

        let now = chrono::Local::now();
        println!(
            "[{}] Current CPU utilization: {:.2}%",
            now.format("%Y-%m-%d %H:%M:%S"),
            cpu_pct
        );

        // 9) Threshold check
        if cpu_pct > args.threshold {
            println!(
                "Threshold exceeded! CPU: {:.2}%, Threshold: {:.2}%",
                cpu_pct, args.threshold
            );
            let trigger_time = Instant::now();

            // a) Drain pre-trigger blobs (the most recent `capacity` entries)
            let drain_start = Instant::now();
            let pre_blobs: Vec<_> = buf.iter().cloned().collect();
            println!("Draining buffer took: {:?}", drain_start.elapsed());

            // b) Timestamped run directory
            let dir_start = Instant::now();
            let ts = Utc::now().format("%Y%m%dT%H%M%S").to_string();
            let run_dir = args.output.join(&ts);
            fs::create_dir_all(&run_dir)?;
            println!("Creating directory took: {:?}", dir_start.elapsed());

            // c) Write out the `.bin` exactly as record does
            let write_start = Instant::now();
            let pre_path = run_dir.join(format!("cpu_utilization_{}.bin", ts));
            let mut f = File::create(&pre_path)?;
            for blob in pre_blobs {
                f.write_all(&blob)?;
            }
            println!("Writing pre-trigger data took: {:?}", write_start.elapsed());

            // d) Hand off to normal post-trigger record
            let record_start = Instant::now();
            let opts = RecordOpts {
                run_name: Some(ts.clone()),
                interval: args.interval,
                period: args.post,
                profile: false,
                profile_java: None,
                pmu_config: None,
            };
            println!(
                "Starting record at: {:?} after trigger",
                record_start.elapsed()
            );
            run_record(&opts, tmp_dir, runlog)?;
            println!("Record completed in: {:?}", record_start.elapsed());

            // e) Move the post-run folder into `<run_dir>/post`
            let move_start = Instant::now();
            let post_src = PathBuf::from(&ts);
            let post_dst = run_dir.join("post");
            if post_src.exists() {
                fs::rename(post_src, post_dst)?;
            }
            println!("Moving post-run folder took: {:?}", move_start.elapsed());

            // Total time from trigger to completion
            println!(
                "Total time from trigger to completion: {:?}",
                trigger_time.elapsed()
            );

            // f) Cooldown before rearming
            thread::sleep(Duration::from_secs(args.cooldown));

            // g) Reset buffer
            buf.clear();
        }
    }
}
