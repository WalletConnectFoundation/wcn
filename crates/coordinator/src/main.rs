#[global_allocator]
static GLOBAL: wc::alloc::Jemalloc = wc::alloc::Jemalloc;

fn main() -> anyhow::Result<()> {
    let _logger = logging::Logger::init(logging::LogFormat::Json, None, None);

    wcn_coordinator::main()
}
