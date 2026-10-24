#[cfg(feature = "opt-loop-profiling")]
#[global_allocator]
static OPT_LOOP_ALLOCATOR: opt_loop_probe::ProbeAllocator = opt_loop_probe::ProbeAllocator::new();

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let worker_threads = std::thread::available_parallelism().map_or(1, usize::from);
    let result = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()?
        .block_on(crate::app::run_with_runtime_threads(worker_threads));

    #[cfg(feature = "opt-loop-profiling")]
    opt_loop_probe::force_flush();

    result
}
