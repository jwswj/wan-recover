pub mod config;
pub mod events;
pub mod gateway;
pub mod meross;
pub mod policy;
pub mod server;
pub mod store;
pub mod worker;

pub fn unix_time() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is before Unix epoch")
        .as_secs()
}
