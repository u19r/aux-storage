use crate::admission::AdmissionConfig;

#[test]
fn runtime_options_builder_retains_typed_admission_configuration() {
    let admission_config = AdmissionConfig {
        enabled: true,
        initial_sustainable_throughput_rps: 8,
        initial_latency_estimate_ms: 10,
        minimum_concurrency: 1,
        maximum_concurrency: 4,
        control_reserve_concurrency: 1,
        queue_capacity: 0,
        max_queue_wait_ms: 100,
    };

    let options = super::DatabaseManagerRuntimeOptions::builder()
        .admission_config(admission_config)
        .build();

    assert_eq!(options.admission_config, admission_config);
}
