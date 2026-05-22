use syn::{Data, DeriveInput, Fields, Type};

pub(crate) fn struct_has_timestamp_millis_field(input_ast: &DeriveInput, field_name: &str) -> bool {
    let Data::Struct(data_struct) = &input_ast.data else {
        return false;
    };
    let Fields::Named(fields) = &data_struct.fields else {
        return false;
    };
    fields.named.iter().any(|field| {
        field
            .ident
            .as_ref()
            .is_some_and(|ident| ident == field_name)
            && type_is_timestamp_millis(&field.ty)
    })
}
pub(crate) fn type_is_timestamp_millis(ty: &Type) -> bool {
    let Type::Path(path) = ty else {
        return false;
    };
    path.path
        .segments
        .last()
        .is_some_and(|segment| segment.ident == "TimestampMillis")
}
