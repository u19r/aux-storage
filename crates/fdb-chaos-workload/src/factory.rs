use crate::{
    common::*,
    imports::*,
    kv_smoke::KvSmokeWorkload,
    noop::{InvalidWorkload, NoopWorkload},
    partition_family::PartitionFamilyWorkload,
    pubsub_delivery::PubsubDeliveryWorkload,
    queue_visibility::QueueVisibilityWorkload,
    read_sequence_dag::ReadSequenceDagWorkload,
    table_atomicity::TableAtomicityWorkload,
};

struct AuxStorageFdbChaosFactory;

impl RustWorkloadFactory for AuxStorageFdbChaosFactory {
    fn create(name: String, context: WorkloadContext) -> WrappedWorkload {
        match name.as_str() {
            WORKLOAD_KV_SMOKE => KvSmokeWorkload::new(name, context).wrap(),
            WORKLOAD_NOOP => NoopWorkload::new(name, context).wrap(),
            WORKLOAD_PARTITION_FAMILY => PartitionFamilyWorkload::new(name, context).wrap(),
            WORKLOAD_PUBSUB_DELIVERY => PubsubDeliveryWorkload::new(name, context).wrap(),
            WORKLOAD_QUEUE_VISIBILITY => QueueVisibilityWorkload::new(name, context).wrap(),
            WORKLOAD_READ_SEQUENCE_DAG => ReadSequenceDagWorkload::new(name, context).wrap(),
            WORKLOAD_TABLE_ATOMICITY => TableAtomicityWorkload::new(name, context).wrap(),
            _ => InvalidWorkload::new(name, context).wrap(),
        }
    }
}

register_factory!(AuxStorageFdbChaosFactory);
