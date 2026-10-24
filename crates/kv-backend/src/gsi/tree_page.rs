//! Seek and ordered traversal of compact roots in one native snapshot.
use std::num::{NonZeroU32, NonZeroUsize};

use storage_types::{BoundTableInfo, IndexName, ReadProtection, StorageResult};

use super::{
    tree_codec::{Branch, NodeId, Root, bit, corrupt, divergence},
    tree_read::{LeafInput, VerifiedLeaf, hydrate_leaf, load_leaf, load_leaf_value, require_path},
    write::{HydratedProjection, projection_read_work},
};
use crate::{
    KvAttempt, KvEntry, KvRange, KvRangePage, catalog::KvCatalogAttempt, range::prefix_upper_bound,
};

struct Route {
    bit: usize,
    side: usize,
    parent: Option<usize>,
}
struct Visit {
    node: NodeId,
    path: Option<usize>,
}
struct TreeCursor {
    routes: Vec<Route>,
    stack: Vec<Visit>,
    seek_leaf: Option<(NodeId, VerifiedLeaf)>,
}
struct Partition<'a> {
    table: &'a BoundTableInfo,
    index: &'a IndexName,
    scope: [u8; 6],
    digest: &'a [u8],
}
struct ProjectionPage {
    physical: KvRangePage,
    images: Vec<HydratedProjection>,
    budget: stream_provider::retained_image::RetainedImagePageBudget,
}

pub(crate) async fn read_tree_page<A: KvAttempt>(
    catalog: &mut KvCatalogAttempt<'_, A>,
    table: &BoundTableInfo,
    index: &IndexName,
    range: KvRange,
) -> StorageResult<(KvRangePage, Vec<HydratedProjection>)> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(catalog);
    let catalog = settlement.as_mut();
    let result = async {
        let mut roots = root_span(&range)?;
        let mut result = ProjectionPage {
            physical: KvRangePage {
                entries: Vec::new(),
                more: false,
            },
            images: Vec::new(),
            budget: stream_provider::retained_image::RetainedImagePageBudget::try_new(
                range.max_bytes.get(),
            )?,
        };
        while roots.begin < roots.end {
            let page = catalog
                .attempt
                .scan_unchunked(
                    roots.clone(),
                    catalog.protection,
                    NonZeroUsize::MIN.saturating_add(41),
                )
                .await?;
            if page.entries.len() > 1 || (page.entries.is_empty() && page.more) {
                return Err(corrupt());
            }
            let Some(entry) = page.entries.into_iter().next() else {
                break;
            };
            if entry.key.len() != 39
                || entry.key < roots.begin
                || entry.key >= roots.end
                || entry.key[0] != crate::constants::GSI_PROJECTION_FAMILY
            {
                return Err(corrupt());
            }
            let partition = Partition {
                table,
                index,
                scope: entry.key[1..7].try_into().map_err(|_| corrupt())?,
                digest: &entry.key[7..],
            };
            let root = Root::decode(&entry.value)?;
            if result
                .read_partition(catalog, &partition, root.child, &range)
                .await?
            {
                result.physical.more = true;
                break;
            }
            if !page.more {
                break;
            }
            if range.reverse {
                roots.end = entry.key;
            } else {
                roots.begin = prefix_upper_bound(&entry.key).ok_or_else(corrupt)?;
            }
        }
        Ok((result.physical, result.images))
    }
    .await;
    settlement.complete(result)
}

fn root_span(range: &KvRange) -> StorageResult<KvRange> {
    let begin = range.begin[..range.begin.len().min(39)].to_vec();
    let end = if range.end.len() > 39 {
        prefix_upper_bound(&range.end[..39]).ok_or_else(corrupt)?
    } else {
        range.end.clone()
    };
    Ok(KvRange {
        entry_bounds: None,
        begin,
        end,
        reverse: range.reverse,
        limit: NonZeroU32::MIN,
        max_bytes: NonZeroUsize::MIN.saturating_add(81),
    })
}

impl ProjectionPage {
    async fn read_partition<A: KvAttempt>(
        &mut self,
        catalog: &mut KvCatalogAttempt<'_, A>,
        partition: &Partition<'_>,
        root: NodeId,
        range: &KvRange,
    ) -> StorageResult<bool> {
        let result = self;
        let bound = if range.reverse {
            &range.end
        } else {
            &range.begin
        };
        let prefix = [
            vec![crate::constants::GSI_PROJECTION_FAMILY],
            partition.scope.to_vec(),
        ]
        .concat();
        let mut word = bound.strip_prefix(prefix.as_slice());
        if word.is_none()
            && ((!range.reverse && bound > &prefix) || (range.reverse && bound <= &prefix))
        {
            return Ok(false);
        }
        if let Some(bound_word) = word
            && !bound_word.starts_with(partition.digest)
        {
            if (partition.digest < bound_word) != range.reverse {
                return Ok(false);
            }
            word = None;
        }
        let mut cursor = seek_partition(catalog, partition, root, word, range.reverse).await?;
        while let Some(visit) = cursor.stack.pop() {
            if matches!(visit.node, NodeId::Branch(_)) {
                expand_branch(catalog, partition, &mut cursor, visit, range.reverse).await?;
                continue;
            }
            if result.physical.entries.len() >= range.limit.get() as usize {
                return Ok(true);
            }
            let value = match cursor.seek_leaf.as_ref() {
                Some((node, leaf)) if *node == visit.node => leaf.value.clone(),
                _ => load_leaf_value(catalog, &partition.scope, visit.node).await?,
            };
            let work = projection_read_work(partition.table, &value)?;
            let mut budget = result.budget;
            if !budget.admit(work)? {
                return Ok(true);
            }
            let prepared = if cursor
                .seek_leaf
                .as_ref()
                .is_some_and(|(node, _)| *node == visit.node)
            {
                cursor.seek_leaf.take()
            } else {
                None
            };
            let leaf = match prepared {
                Some((node, leaf)) if node == visit.node => leaf,
                _ => {
                    hydrate_leaf(
                        catalog,
                        LeafInput {
                            table: partition.table,
                            index: partition.index,
                            scope: &partition.scope,
                            node: visit.node,
                        },
                        value,
                    )
                    .await?
                }
            };
            verify_visit(partition, (&cursor, &visit), &leaf.word)?;
            let key = [
                vec![crate::constants::GSI_PROJECTION_FAMILY],
                partition.scope.to_vec(),
                leaf.word,
            ]
            .concat();
            if key < range.begin || key >= range.end {
                break;
            }
            result.budget = budget;
            result.physical.entries.push(KvEntry::new(key, leaf.value));
            result.images.push(leaf.image);
        }
        Ok(false)
    }
}

async fn seek_partition<A: KvAttempt>(
    catalog: &mut KvCatalogAttempt<'_, A>,
    partition: &Partition<'_>,
    root: NodeId,
    bound: Option<&[u8]>,
    reverse: bool,
) -> StorageResult<TreeCursor> {
    let Some(bound) = bound else {
        return Ok(TreeCursor {
            routes: Vec::new(),
            stack: vec![Visit {
                node: root,
                path: None,
            }],
            seek_leaf: None,
        });
    };
    let path = catalog
        .projection_tree
        .descend(catalog.attempt, &partition.scope, root, bound)
        .await?;
    let leaf = load_leaf(
        catalog,
        LeafInput {
            table: partition.table,
            index: partition.index,
            scope: &partition.scope,
            node: path.leaf,
        },
    )
    .await?;
    require_path(&path, &leaf.word, partition.digest)?;
    let divergence = if bound == leaf.word {
        usize::MAX
    } else {
        divergence(bound, &leaf.word)?
    };
    let split = path
        .steps
        .iter()
        .position(|step| step.branch.bit >= divergence)
        .unwrap_or(path.steps.len());
    let mut routes = Vec::new();
    let mut stack = Vec::new();
    let mut ancestors = None;
    for step in &path.steps[..split] {
        let next_side = usize::from(!reverse);
        if step.side != next_side {
            let sibling_path = Some(routes.len());
            routes.push(Route {
                bit: step.branch.bit,
                side: next_side,
                parent: ancestors,
            });
            stack.push(Visit {
                node: step.branch.children[next_side],
                path: sibling_path,
            });
        }
        routes.push(Route {
            bit: step.branch.bit,
            side: step.side,
            parent: ancestors,
        });
        ancestors = Some(routes.len() - 1);
    }
    if (!reverse && leaf.word.as_slice() >= bound) || (reverse && leaf.word.as_slice() < bound) {
        stack.push(Visit {
            node: path.steps.get(split).map_or(path.leaf, |step| step.id),
            path: ancestors,
        });
    }
    Ok(TreeCursor {
        routes,
        stack,
        seek_leaf: Some((path.leaf, leaf)),
    })
}

async fn expand_branch<A: KvAttempt>(
    catalog: &KvCatalogAttempt<'_, A>,
    partition: &Partition<'_>,
    cursor: &mut TreeCursor,
    visit: Visit,
    reverse: bool,
) -> StorageResult<()> {
    let bytes = catalog
        .projection_tree
        .load(
            catalog.attempt,
            visit.node.key(&partition.scope),
            ReadProtection::Snapshot,
            71,
        )
        .await?
        .ok_or_else(corrupt)?;
    let branch = Branch::decode(
        &bytes,
        visit.path.map(|position| cursor.routes[position].bit),
    )?;
    let first = usize::from(reverse);
    for side in [1 - first, first] {
        let path = Some(cursor.routes.len());
        cursor.routes.push(Route {
            bit: branch.bit,
            side,
            parent: visit.path,
        });
        cursor.stack.push(Visit {
            node: branch.children[side],
            path,
        });
    }
    Ok(())
}

fn verify_visit(
    partition: &Partition<'_>,
    position: (&TreeCursor, &Visit),
    word: &[u8],
) -> StorageResult<()> {
    if word.get(..32) != Some(partition.digest) {
        return Err(corrupt());
    }
    let (cursor, visit) = position;
    let mut route = visit.path;
    while let Some(position) = route {
        let step = cursor.routes.get(position).ok_or_else(corrupt)?;
        if bit(word, step.bit) != step.side {
            return Err(corrupt());
        }
        route = step.parent;
    }
    Ok(())
}
