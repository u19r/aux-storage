use crate::{
    constants::*,
    imports::*,
    kv_smoke::KvSmokeWorkload,
    noop::{InvalidWorkload, NoopWorkload},
    pubsub_delivery::PubsubDeliveryWorkload,
    queue_claim::QueueClaimWorkload,
    queue_dead_letter::QueueDeadLetterWorkload,
    queue_visibility::QueueVisibilityWorkload,
    queue_wake::QueueWakeWorkload,
    read_sequence_dag::ReadSequenceDagWorkload,
    table_atomicity::TableAtomicityWorkload,
    table_recreate::TableRecreateWorkload,
};

struct AuxStorageFdbChaosFactory;

impl RustWorkloadFactory for AuxStorageFdbChaosFactory {
    fn create(name: String, context: WorkloadContext) -> WrappedWorkload {
        crate::ambient_source::install_simulation_ambient(&context);
        match name.as_str() {
            WORKLOAD_KV_SMOKE => KvSmokeWorkload::new(name, context).wrap(),
            WORKLOAD_NOOP => NoopWorkload::new(name, context).wrap(),
            WORKLOAD_PUBSUB_DELIVERY => PubsubDeliveryWorkload::new(name, context).wrap(),
            WORKLOAD_QUEUE_DEAD_LETTER => QueueDeadLetterWorkload::new(name, context).wrap(),
            WORKLOAD_QUEUE_WAKE => QueueWakeWorkload::new(name, context).wrap(),
            WORKLOAD_TABLE_RECREATE => TableRecreateWorkload::new(name, context).wrap(),
            WORKLOAD_QUEUE_CLAIM => QueueClaimWorkload::new(name, context).wrap(),
            WORKLOAD_QUEUE_VISIBILITY => QueueVisibilityWorkload::new(name, context).wrap(),
            WORKLOAD_READ_SEQUENCE_DAG => ReadSequenceDagWorkload::new(name, context).wrap(),
            WORKLOAD_TABLE_ATOMICITY => TableAtomicityWorkload::new(name, context).wrap(),
            _ => InvalidWorkload::new(name, context).wrap(),
        }
    }
}

register_factory!(AuxStorageFdbChaosFactory);
