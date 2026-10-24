#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    crate::app::run().await
}
