pub(crate) const REGIONAL_RECORD_NAMESPACE: &str = "regional-replication";
pub(crate) const ORIGIN_LAYOUT_SUFFIX: &[u8] = b"layout";
pub(crate) const PARTITION_SUFFIX: &[u8] = b"partition/";
pub(crate) const EVENT_SUFFIX: &[u8] = b"event/";
pub(crate) const ACCEPTANCE_SUFFIX: &[u8] = b"acceptance/";
pub(crate) const SOURCE_SUFFIX: &[u8] = b"source/";
pub(crate) const IMPORT_PAGE_SUFFIX: &[u8] = b"import-page/";
pub(crate) const RETAINED_POSITION_SUFFIX: &[u8] = b"retained-position/";
pub(crate) const IMPORT_MANIFEST_SUFFIX: &[u8] = b"import-manifest/";
pub(crate) const TABLE_STATE_SUFFIX: &[u8] = b"table-state/";
pub(crate) const CAPTURE_SUFFIX: &[u8] = b"capture/";
pub(crate) const OPERATION_SUFFIX: &[u8] = b"operation/";

pub(crate) const TRANSPORT_MAGIC: [u8; 4] = *b"AUXR";
pub(crate) const TRANSPORT_VERSION: u16 = 33;
pub(crate) const TRANSPORT_HEADER_BYTES: usize = 16;
pub(crate) const TRANSPORT_DATA_KIND: u8 = 1;
pub(crate) const TRANSPORT_REPLY_KIND: u8 = 3;
pub(crate) const TRANSPORT_ZLIB: u8 = 1;

pub(crate) const DELIVERY_PROGRESS_SUFFIX: &[u8] = b"delivery-progress/";
pub(crate) const DELIVERY_RECEIPT_SUFFIX: &[u8] = b"delivery-receipt/";

pub(crate) const DELIVERY_JOB_KIND: &str = "regional-delivery";

pub(crate) const CONTROL_SLOT_SUFFIX: &[u8] = b"control-slot/";
pub(crate) const CONTROL_OPERATION_SUFFIX: &[u8] = b"control-operation/";

pub(crate) const TRANSPORT_CONTROL_KIND: u8 = 2;

pub(crate) const CONTROL_JOB_KIND: &str = "regional-control";

pub(crate) const BASELINE_SUFFIX: &[u8] = b"baseline/";

pub(crate) const SOURCE_CAPTURE_SUFFIX: &[u8] = b"source-capture/";
pub(crate) const SOURCE_HOLD_SUFFIX: &[u8] = b"source-hold/";
pub(crate) const SOURCE_CAPTURE_JOB_KIND: &str = "regional-source-capture";
pub(crate) const TARGET_BOOTSTRAP_SUFFIX: &[u8] = b"target-bootstrap/";
pub(crate) const BASELINE_COPY_SUFFIX: &[u8] = b"baseline-copy/";
pub(crate) const TRANSPORT_BOOTSTRAP_KIND: u8 = 4;

pub(crate) const SOURCE_COPY_SUFFIX: &[u8] = b"source-copy/";

pub(crate) const TARGET_CUT_SUFFIX: &[u8] = b"target-cut/";

pub(crate) const SOURCE_COPY_PAGE_SUFFIX: &[u8] = b"source-copy-page/";

pub(crate) const SOURCE_TRANSFER_SUFFIX: &[u8] = b"source-transfer/";
pub(crate) const ADOPTED_TRANSFER_SUFFIX: &[u8] = b"adopted-transfer/";
pub(crate) const ADOPTED_RETIRED_CUT_SUFFIX: &[u8] = b"adopted-retired-cut/";
pub(crate) const SOURCE_COLLECTION_DECLARATION_SUFFIX: &[u8] = b"source-collection-declaration/";
pub(crate) const ADDITION_OPERATION_SUFFIX: &[u8] = b"addition-operation/";
pub(crate) const ADDITION_PENDING_SUFFIX: &[u8] = b"addition-pending/";
pub(crate) const ADDITION_TABLE_SUFFIX: &[u8] = b"addition-table/";
pub(crate) const ADDITION_RECOVERY_SUFFIX: &[u8] = b"addition-recovery/";
pub(crate) const ADDITION_JOB_KIND: &str = "regional-addition";
pub(crate) const BOOTSTRAP_TRANSFER_JOB_KIND: &str = "regional-bootstrap-transfer";

pub(crate) const RETAINED_LINK_SUFFIX: &[u8] = b"retained-link/";
pub(crate) const EXPECTED_WINNER_SUFFIX: &[u8] = b"expected-winner/";

pub(crate) const PROJECTION_OWNER_SUFFIX: &[u8] = b"projection-owner/";

pub(crate) const ENABLE_JOB_KIND: &str = "regional-enable";
pub(crate) const ENABLE_OPERATION_SUFFIX: &[u8] = b"enable-operation/";

pub(crate) const ENABLE_PENDING_SUFFIX: &[u8] = b"enable-pending/";
pub(crate) const ENABLE_RECOVERY_SUFFIX: &[u8] = b"enable-recovery-cursor";

pub(crate) const TABLE_SEAL_SUFFIX: &[u8] = b"table-seal/";
pub(crate) const TABLE_SEAL_CUT_SUFFIX: &[u8] = b"table-seal-cut/";

pub(crate) const CONFIGURATION_OPERATION_SUFFIX: &[u8] = b"configuration-operation/";
pub(crate) const CONTROL_PENDING_SUFFIX: &[u8] = b"control-pending/";
pub(crate) const CONTROL_RECOVERY_SUFFIX: &[u8] = b"control-recovery-cursor";
pub(crate) const CONTROL_TABLE_SUFFIX: &[u8] = b"control-table/";
pub(crate) const CONTROL_REMOVAL_TRANSFER_SUFFIX: &[u8] = b"control-removal-transfer/";

pub(crate) const DELIVERY_ACTIVITY_SUFFIX: &[u8] = b"delivery-activity/";
pub(crate) const DELIVERY_RECOVERY_SUFFIX: &[u8] = b"delivery-recovery-cursor";

pub(crate) const IMPORT_COVERAGE_SUFFIX: &[u8] = b"import-coverage/";
pub(crate) const RECEIVED_SEAL_SUFFIX: &[u8] = b"received-seal/";
pub(crate) const RETIRED_ORIGIN_SUFFIX: &[u8] = b"retired-origin/";
pub(crate) const TRANSPORT_SEAL_KIND: u8 = 5;

pub(crate) const HISTORY_CAPTURE_SUFFIX: &[u8] = b"history-capture/";
pub(crate) const TRANSPORT_HISTORY_CAPTURE_KIND: u8 = 6;

pub(crate) const HISTORY_COLLECTION_SUFFIX: &[u8] = b"history-collection/";
pub(crate) const COLLECTED_HISTORY_DEFINITION_SUFFIX: &[u8] = b"collected-history-definition/";
pub(crate) const COLLECTED_HISTORY_PAGE_SUFFIX: &[u8] = b"collected-history-page/";

pub(crate) const BASELINE_LINEAGE_AUTHORITY_SUFFIX: &[u8] = b"baseline-lineage-authority/";

pub(crate) const RETIRED_SOURCE_CATALOG_SUFFIX: &[u8] = b"retired-source-catalog/";

pub(crate) const COLLECTED_CATALOG_SUFFIX: &[u8] = b"collected-catalog/";
pub(crate) const COLLECTED_RETIRED_PAGE_SUFFIX: &[u8] = b"collected-retired-page/";
pub(crate) const RETAINED_TRANSFER_PROGRESS_SUFFIX: &[u8] = b"retained-transfer-progress/";
pub(crate) const COLLECTED_RETAINED_DEFINITION_SUFFIX: &[u8] = b"collected-retained-definition/";
pub(crate) const COLLECTED_RETAINED_PAGE_SUFFIX: &[u8] = b"collected-retained-page/";
pub(crate) const COLLECTED_BASELINE_DEFINITION_SUFFIX: &[u8] = b"collected-baseline-definition/";

pub(crate) const HISTORY_CUT_PAGE_WIDTH: usize = 64;

pub(crate) const TARGET_CANCELLATION_SUFFIX: &[u8] = b"target-cancellation/";

pub(crate) const REGIONAL_LAYOUT_VERSION: u16 = 107;
pub(crate) const NATIVE_JOURNAL_BINDING_RESOURCE: &[u8] = b"native-database";
pub(crate) const NATIVE_JOURNAL_BINDING_SUFFIX: &[u8] = b"binding";
pub(crate) const NATIVE_ARCHIVE_BINDING_SUFFIX: &[u8] = b"archive-binding";
pub(crate) const HISTORY_CAPACITY_ROOT_GENERATION: u64 = 0;
pub(crate) const HISTORY_CAPACITY_MARKER_SUFFIX: &[u8] = b"history-capacity-initialized/";
pub(crate) const HISTORY_CAPACITY_SUFFIX: &[u8] = b"history-capacity/";
pub(crate) const IMPORT_FRAGMENT_PENDING_SUFFIX: &[u8] = b"import-fragment-pending/";

pub(crate) const TRANSPORT_CHUNK_DECLARATION_KIND: u8 = 7;
pub(crate) const TRANSPORT_CHUNK_KIND: u8 = 8;

pub(crate) const CANONICAL_VIEW_SUFFIX: &[u8] = b"canonical-view/";
pub(crate) const CANONICAL_CHECKPOINT_SUFFIX: &[u8] = b"canonical-checkpoint/";
pub(crate) const PORTABLE_CHECKPOINT_SUFFIX: &[u8] = b"portable-checkpoint/";
pub(crate) const READER_HOLD_SUFFIX: &[u8] = b"reader-hold/";
pub(crate) const READER_RETENTION_EPOCH_SUFFIX: &[u8] = b"reader-retention-epoch/";
pub(crate) const HISTORY_RETENTION_EPOCH_SUFFIX: &[u8] = b"history-retention-epoch/";

pub(crate) const PUBLICATION_LOOKUP_SUFFIX: &[u8] = b"publication-lookup/";

pub(crate) const READER_EXPIRY_SUFFIX: &[u8] = b"reader-expiry/";
pub(crate) const READER_EXPIRY_JOB_KIND: &str = "regional-reader-expiry";
pub(crate) const READER_EXPIRY_IDLE_INTERVAL_MS: u64 = 300_000;

pub(crate) const CAUSAL_STREAM_SUFFIX: &[u8] = b"causal-stream/";
pub(crate) const CAUSAL_STACK_SUFFIX: &[u8] = b"causal-stack/";
pub(crate) const CAUSAL_VISITED_SUFFIX: &[u8] = b"causal-visited/";

pub(crate) const HISTORY_IMAGE_CHARGE_SUFFIX: &[u8] = b"capacity-image/";

pub(crate) const REENABLE_INVENTORY_SUFFIX: &[u8] = b"reenable-inventory/";
pub(crate) const REENABLE_CUT_SUFFIX: &[u8] = b"reenable-cut/";
pub(crate) const RETAINED_LINEAGE_SUFFIX: &[u8] = b"retained-lineage/";

pub(crate) const TARGET_RETAINED_PAGE_SUFFIX: &[u8] = b"target-retained-page";

pub(crate) const REENABLE_BASELINE_INVENTORY_SUFFIX: &[u8] = b"reenable-baseline-inventory/";
pub(crate) const REENABLE_BASELINE_SOURCE_SUFFIX: &[u8] = b"reenable-baseline-source/";
pub(crate) const REENABLE_BASELINE_TARGET_SUFFIX: &[u8] = b"reenable-baseline-target/";
pub(crate) const REENABLE_BASELINE_RECEIPT_SUFFIX: &[u8] = b"reenable-baseline-receipt/";

pub(crate) const RETAINED_CATALOG_SUFFIX: &[u8] = b"retained-catalog/";
pub(crate) const RETAINED_CATALOG_LINEAGE_SUFFIX: &[u8] = b"retained-catalog-lineage/";
pub(crate) const RETAINED_CATALOG_ACTIVATION_SUFFIX: &[u8] = b"retained-catalog-activation/";

pub(crate) const ARCHIVE_FAMILY_SUFFIX: &[u8] = b"archive-family";
pub(crate) const ARCHIVE_SCAN_SUFFIX: &[u8] = b"archive-scan";
pub(crate) const ARCHIVE_STATE_SUFFIX: &[u8] = b"archive-state";
pub(crate) const ARCHIVE_POINTER_SUFFIX: &[u8] = b"archive-pointer/";
pub(crate) const ARCHIVE_PUBLICATION_COVERAGE_SUFFIX: &[u8] = b"archive-publication-coverage/";
pub(crate) const ARCHIVE_JOB_KIND: &str = "regional-archive";
pub(crate) const ARCHIVE_IDENTITY_FILE: &str = ".aux-storage-archive";
pub(crate) const ARCHIVE_IDENTITY_FORMAT: &[u8; 8] = b"AXARCH02";

pub(crate) const PUBLIC_TRIM_HEAD_SUFFIX: &[u8] = b"public-trim-head/";

pub(crate) const PUBLIC_TRIM_PROPOSAL_SUFFIX: &[u8] = b"public-trim-proposal/";
pub(crate) const PUBLIC_TRIM_OPERATION_SUFFIX: &[u8] = b"public-trim-operation/";

pub(crate) const PUBLIC_TRIM_SOURCE_SCAN_SUFFIX: &[u8] = b"public-trim-source-scan/";
pub(crate) const PUBLIC_TRIM_PREPARATION_SUFFIX: &[u8] = b"public-trim-preparation/";
pub(crate) const PUBLIC_TRIM_GRANT_TURN_SUFFIX: &[u8] = b"public-trim-grant-turn/";
/// Ordered acquisition permits one live critical section per native journal.
pub(crate) const MAX_ACTIVE_TRIM_RESERVATIONS: u64 = 1;

pub(crate) const TRANSPORT_TRIM_KIND: u8 = 9;

pub(crate) const TRIM_RESERVATION_SUFFIX: &[u8] = b"trim-reservation/";
pub(crate) const TRIM_RESERVATION_INDEX_SUFFIX: &[u8] = b"trim-reservation-index/";

pub(crate) const PUBLIC_TRIM_COMMIT_SUFFIX: &[u8] = b"public-trim-commit/";

pub(crate) const PUBLIC_TRIM_INSTALLATION_SUFFIX: &[u8] = b"public-trim-installation/";

pub(crate) const PUBLIC_TRIM_COMPLETION_SUFFIX: &[u8] = b"public-trim-completion/";

pub(crate) const ARCHIVE_HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";
