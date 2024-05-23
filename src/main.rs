fn main() {
    dotenv::dotenv().ok();

    if let Err(err) = univ3_pool_states::run() {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}
