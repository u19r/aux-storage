use crate::{
    common::*,
    imports::*,
    kv_smoke::KvSmokeWorkload,
    noop::{InvalidWorkload, NoopWorkload},
    partition_family::PartitionFamilyWorkload,
    pubsub_delivery::PubsubDeliveryWorkload,
    queue_visibility::QueueVisibilityWorkload,
    table_atomicity::TableAtomicityWorkload,
};

struct AuxStorageFdbChaosFactory;

impl RustWorkloadFactory for AuxStorageFdbChaosFactory {
    fn create(name: String, context: WorkloadContext) -> WrappedWorkload {
        match name.as_str() {
            WORKLOAD_KV_SMOKE => WrappedWorkload::new(KvSmokeWorkload::new(name, context)),
            WORKLOAD_NOOP => WrappedWorkload::new(NoopWorkload::new(name, context)),
            WORKLOAD_PARTITION_FAMILY => {
                WrappedWorkload::new(PartitionFamilyWorkload::new(name, context))
            }
            WORKLOAD_PUBSUB_DELIVERY => {
                WrappedWorkload::new(PubsubDeliveryWorkload::new(name, context))
            }
            WORKLOAD_QUEUE_VISIBILITY => {
                WrappedWorkload::new(QueueVisibilityWorkload::new(name, context))
            }
            WORKLOAD_TABLE_ATOMICITY => {
                WrappedWorkload::new(TableAtomicityWorkload::new(name, context))
            }
            _ => WrappedWorkload::new(InvalidWorkload::new(name, context)),
        }
    }
}

register_factory!(AuxStorageFdbChaosFactory);
