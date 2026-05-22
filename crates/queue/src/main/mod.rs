mod app;
mod protocol;

#[cfg(feature = "opt-loop-profiling")]
#[global_allocator]
static OPT_LOOP_ALLOCATOR: opt_loop_probe::ProbeAllocator = opt_loop_probe::ProbeAllocator::new();

#[cfg(test)]
pub(crate) use app::{Args, QueueStorageArg, queue_config_from_args, queue_url};
#[cfg(test)]
pub(crate) use protocol::add_common_headers;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let worker_threads = std::thread::available_parallelism().map_or(1, usize::from);
    let result = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()?
        .block_on(app::run_with_runtime_threads(worker_threads));

    #[cfg(feature = "opt-loop-profiling")]
    opt_loop_probe::force_flush();

    result
}

#[cfg(test)]
mod backend_builder_tests;
#[cfg(test)]
mod main_tests;
#[cfg(test)]
mod protocol_tests;
