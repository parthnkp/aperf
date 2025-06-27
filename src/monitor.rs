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
    time::{Duration, Instant},
};

use crate::data::{CollectData, CollectorParams};
use crate::record::{
    collect_static_data, prepare_data_collectors, record as run_record, Record as RecordOpts,
};
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

    // 2) Pre‐warm the normal APerf collectors **once** for the post-trigger run:
    let mut prep_params = InitParams::new(String::new());
    prep_params.period = args.post;
    prep_params.interval = args.interval;
    prep_params.tmp_dir = tmp_dir.to_path_buf();
    prep_params.runlog = runlog.to_path_buf();
    PERFORMANCE_DATA.lock().unwrap().set_params(prep_params);
    // create run_dir, open all .bin files
    PERFORMANCE_DATA.lock().unwrap().init_collectors()?;
    // heavy perf-event and procfs setup
    prepare_data_collectors()?;
    // static data (system_info, kernel_config, etc.)
    collect_static_data()?;

    // 3) Now set up our in-memory circular buffers
    let capacity = (args.period / args.interval) as usize;
    let mut buf_cpu = VecDeque::with_capacity(capacity);
    let mut buf_disk = VecDeque::with_capacity(capacity);
    let mut buf_vm = VecDeque::with_capacity(capacity);
    let mut buf_mem = VecDeque::with_capacity(capacity);
    let mut buf_intr = VecDeque::with_capacity(capacity);
    let mut buf_net = VecDeque::with_capacity(capacity);
    let mut buf_proc = VecDeque::with_capacity(capacity);
    let mut buf_sysctl = VecDeque::with_capacity(capacity);

    // 4) Instantiate and prime each Raw collector
    let mut cpu_raw = CpuUtilizationRaw::new();
    let mut disk_raw = DiskstatsRaw::new();
    let mut vm_raw = VmstatRaw::new();
    let mut mem_raw = MeminfoDataRaw::new();
    let mut intr_raw = InterruptDataRaw::new();
    let mut net_raw = NetstatRaw::new();
    let mut proc_raw = ProcessesRaw::new();
    let mut sysctl_raw = SysctlData::new();

    let params = CollectorParams::new();
    cpu_raw.collect_data(&params)?;
    disk_raw.collect_data(&params)?;
    vm_raw.collect_data(&params)?;
    mem_raw.collect_data(&params)?;
    intr_raw.collect_data(&params)?;
    net_raw.collect_data(&params)?;
    proc_raw.collect_data(&params)?;
    sysctl_raw.collect_data(&params)?;

    // For the CPU‐usage delta, keep a separate “prev” snapshot
    let mut prev_cpu = cpu_raw.clone();

    // 5) Enter the sampling loop
    loop {
        thread::sleep(Duration::from_secs(args.interval));

        serialize_and_buffer(&mut cpu_raw, &mut buf_cpu, &params, capacity)?;
        serialize_and_buffer(&mut disk_raw, &mut buf_disk, &params, capacity)?;
        serialize_and_buffer(&mut vm_raw, &mut buf_vm, &params, capacity)?;
        serialize_and_buffer(&mut mem_raw, &mut buf_mem, &params, capacity)?;
        serialize_and_buffer(&mut intr_raw, &mut buf_intr, &params, capacity)?;
        serialize_and_buffer(&mut net_raw, &mut buf_net, &params, capacity)?;
        serialize_and_buffer(&mut proc_raw, &mut buf_proc, &params, capacity)?;
        serialize_and_buffer(&mut sysctl_raw, &mut buf_sysctl, &params, capacity)?;

        // CPU% next snapshot
        let cpu_pct = {
            let mut curr = cpu_raw.clone();
            curr.collect_data(&params)?;
            let pct = calculate_cpu_usage(&prev_cpu, &curr)?;
            prev_cpu = curr;
            pct
        };
        println!(
            "[{}] Current CPU utilization: {:.2}%",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
            cpu_pct
        );

        // 6) On trigger, dump the pre-buffers and fire the fast post-record
        if cpu_pct > args.cpu_usage {
            println!(
                "Threshold exceeded: {:.2}% > {:.2}%",
                cpu_pct, args.cpu_usage
            );
            let ts = Utc::now().format("%Y%m%dT%H%M%S").to_string();
            let run_dir = args.output.join(&ts);
            fs::create_dir_all(&run_dir)?;

            macro_rules! dump {
                ($buf:ident, $name:expr) => {{
                    let mut f = File::create(run_dir.join(format!("{}_{}.bin", $name, ts)))?;
                    for blob in &$buf {
                        f.write_all(blob)?;
                    }
                }};
            }

            dump!(buf_cpu, "cpu_utilization");
            dump!(buf_disk, "disk_stats");
            dump!(buf_vm, "vmstat");
            dump!(buf_mem, "meminfo");
            dump!(buf_intr, "interrupts");
            dump!(buf_net, "netstat");
            dump!(buf_proc, "processes");
            dump!(buf_sysctl, "sysctl");

            // now run the **fast** post-trigger record (prep was already done)
            run_record(
                &RecordOpts {
                    run_name: Some(ts.clone()),
                    interval: args.interval,
                    period: args.post,
                    profile: false,
                    profile_java: None,
                    pmu_config: None,
                    skip_prep: true,
                },
                tmp_dir,
                runlog,
            )?;

            // move the post-run into <run_dir>/post
            let _ = fs::rename(ts, run_dir.join("post"));
            thread::sleep(Duration::from_secs(args.cooldown));

            // clear buffers for next round
            buf_cpu.clear();
            buf_disk.clear();
            buf_vm.clear();
            buf_mem.clear();
            buf_intr.clear();
            buf_net.clear();
            buf_proc.clear();
            buf_sysctl.clear();
        }
    }
}
