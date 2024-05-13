fn main() {
    dotenv::dotenv().ok();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    if let Err(err) = rt.block_on(univ3_pool_states::run(rt.handle().clone())) {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}
