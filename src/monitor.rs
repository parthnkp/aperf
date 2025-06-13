use anyhow::{anyhow, bail, Result};
use chrono::Utc;
use clap::Args;
use ringbuf::RingBuffer;
use serde_json::to_writer_pretty;
use std::{
    fs,
    fs::File,
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

// Types & functions from existing modules:
use crate::data::cpu_utilization::get_aggregate_data;
use crate::data::cpu_utilization::{CpuUtilization, CpuUtilizationRaw};
use crate::data::{CollectData, CollectorParams, Data, ProcessedData};
use crate::record::{record as run_record, Record as RecordOpts};
use crate::utils::DataMetrics;
use crate::visualizer::GetData;

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
fn calculate_cpu_usage(prev_raw: &CpuUtilizationRaw, curr_raw: &CpuUtilizationRaw) -> Result<f32> {
    // Wrap in Data enum
    let prev = Data::CpuUtilizationRaw(prev_raw.clone());
    let curr = Data::CpuUtilizationRaw(curr_raw.clone());

    // Prepare a metrics holder
    let mut metrics = DataMetrics::new(String::new());

    // Process raw → ProcessedData
    let proc_prev = CpuUtilization::new().process_raw_data(prev)?;
    let proc_curr = CpuUtilization::new().process_raw_data(curr)?;

    // Extract totals
    let util_prev = match proc_prev {
        ProcessedData::CpuUtilization(u) => u.total,
        _ => return Err(anyhow!("Expected CpuUtilization data")),
    };
    let util_curr = match proc_curr {
        ProcessedData::CpuUtilization(u) => u.total,
        _ => return Err(anyhow!("Expected CpuUtilization data")),
    };

    // Build the Vec<CpuData> for aggregation
    let agg_json = get_aggregate_data(vec![util_prev, util_curr], &mut metrics)?;
    let cpu_data: Vec<crate::data::cpu_utilization::CpuData> = serde_json::from_str(&agg_json)?;

    // Idle% is in `.values.idle`; busy% = 100 - idle%
    if let Some(last) = cpu_data.last() {
        Ok(100.0 - last.values.idle as f32)
    } else {
        Ok(0.0)
    }
}

pub fn monitor(args: &MonitorArgs, tmp_dir: &Path, runlog: &Path) -> Result<()> {
    // Sanity checks
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

    // Prepare ring buffer
    let capacity = (args.period / args.interval) as usize;
    let ringbuf = RingBuffer::<f32>::new(capacity);
    let (mut producer, mut consumer) = ringbuf.split();

    // Prime the “previous raw” by taking one snapshot
    let mut prev_raw = CpuUtilizationRaw::new();
    let params = CollectorParams::new();
    prev_raw.collect_data(&params)?; // requires `CollectData` trait in scope

    loop {
        // 1) Sleep until next tick
        thread::sleep(Duration::from_secs(args.interval));

        // 2) Take a fresh raw snapshot
        let mut curr_raw = CpuUtilizationRaw::new();
        curr_raw.collect_data(&params)?;

        // 3) Calculate % busy between prev & curr
        let cpu_pct = calculate_cpu_usage(&prev_raw, &curr_raw)?;
        // Update prev for next iteration
        prev_raw = curr_raw;

        // Display current CPU utilization in terminal
        println!("Current CPU utilization: {:.2}%", cpu_pct);

        let _ = producer.push(cpu_pct);

        // 5) Only check once we have a full window
        if producer.len() < capacity {
            continue;
        }

        // 6) Threshold check
        if cpu_pct > args.threshold {
            // a) Drain pre-trigger history
            let mut pre: Vec<f32> = Vec::with_capacity(capacity);
            while let Some(v) = consumer.pop() {
                pre.push(v);
            }

            // b) Timestamped run directory
            let ts = Utc::now().format("%Y%m%dT%H%M%S").to_string();
            let run_dir = args.output.join(&ts);
            fs::create_dir_all(&run_dir)?;

            // c) Dump JSON
            let pre_path = run_dir.join("pre_cpu_util.json");
            let f = File::create(&pre_path)?;
            to_writer_pretty(f, &pre)?;

            // d) Post-trigger record
            let opts = RecordOpts {
                run_name: Some(ts.clone()),
                interval: args.interval,
                period: args.post,
                profile: false,
                profile_java: None,
                pmu_config: None,
            };
            run_record(&opts, tmp_dir, runlog)?;
            let post_src = PathBuf::from(&ts);
            let post_dst = run_dir.join("post");
            if post_src.exists() {
                fs::rename(post_src, post_dst)?;
            }

            thread::sleep(Duration::from_secs(args.cooldown));

            let rb2 = RingBuffer::<f32>::new(capacity);
            let (np, nc) = rb2.split();
            producer = np;
            consumer = nc;
        }
    }
}
