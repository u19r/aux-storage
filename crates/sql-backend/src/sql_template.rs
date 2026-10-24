//! Bind numbers and relation names render from one SQL definition. Root
//! layouts retain static SQL; namespace layouts allocate qualified SQL only.

macro_rules! sql_template {
    ($layout:expr, $($text:literal $( { $index:literal } )? $( [ $relation:literal ] )?),+ ; postgres_suffix($has_suffix:expr, $suffix:literal) $(,)?) => {{
        let layout = $layout;
        let engine = $crate::SqlLayout::engine(layout);
        let sqlite = concat!($($text, $("?", stringify!($index),)? $($relation,)?)*);
        let postgres = if $has_suffix {
            concat!($($text, $("$", stringify!($index),)? $($relation,)?)* $suffix)
        } else {
            concat!($($text, $("$", stringify!($index),)? $($relation,)?)* )
        };
        let ordinary = match engine {
            $crate::SqlEngine::Postgres => postgres,
            $crate::SqlEngine::Sqlite | $crate::SqlEngine::Turso => sqlite,
        };
        $crate::SqlLayout::render_static(layout, ordinary, || match engine {
            $crate::SqlEngine::Postgres if $has_suffix => format!(
                concat!($($text, $("$", stringify!($index),)? $( $crate::sql_template::relation_slot!($relation),)?)* $suffix),
                $($($crate::SqlLayout::relation_name(layout, $relation),)?)*
            ),
            $crate::SqlEngine::Postgres => format!(
                concat!($($text, $("$", stringify!($index),)? $( $crate::sql_template::relation_slot!($relation),)?)*),
                $($($crate::SqlLayout::relation_name(layout, $relation),)?)*
            ),
            $crate::SqlEngine::Sqlite | $crate::SqlEngine::Turso => format!(
                concat!($($text, $("?", stringify!($index),)? $( $crate::sql_template::relation_slot!($relation),)?)*),
                $($($crate::SqlLayout::relation_name(layout, $relation),)?)*
            ),
        })
    }};
    ($layout:expr, $($text:literal $( { $index:literal } )? $( [ $relation:literal ] )?),+ $(,)?) => {
        $crate::sql_template::sql_template!($layout, $($text $( { $index } )? $( [ $relation ] )?),+ ; postgres_suffix(false, ""))
    };
}

macro_rules! relation_slot {
    ($relation:literal) => {
        "{}"
    };
}

pub(crate) use relation_slot;
pub(crate) use sql_template;
