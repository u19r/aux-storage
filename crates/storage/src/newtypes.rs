use storage_provider::StorageProvider;
use stream::StreamProvider;

pub trait DatabaseTrait: StorageProvider + StreamProvider {}
impl<T: StorageProvider + StreamProvider> DatabaseTrait for T {}
