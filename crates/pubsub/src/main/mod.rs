mod app;

#[cfg(test)]
mod app_tests;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    app::run().await
}
