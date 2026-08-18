#![allow(non_snake_case)]

use std::collections::{BTreeMap, BTreeSet};

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;
use storage_types::{
    AttributeValue, GetItemRequest, KeyAttributes, QueryRequest, ReadSequenceConsistency,
    ReadSequenceFromInput, ReadSequenceInputCardinality, ReadSequenceNode, ReadSequenceNodeId,
    ReadSequenceNodeInput, ReadSequenceNodeOperation, ReadSequenceOnMissing, ReadSequencePlan,
    ReadSequenceRequest, ReadSequenceSelector, TableName, plan_read_sequence,
    read_sequence_input_marker,
};

use crate::provider::{
    ReadSequenceMappedOptions, ReadSequencePhysicalDescriptor, ReadSequencePhysicalOperation,
    select_read_sequence_mapped_edges,
};

const NODE_COUNT: usize = 6;
const NORMAL_EDGES: &[i64] = &[2, 12, 23, 24, 35];

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct ReadSequenceDagState {
    #[serde(rename = "activeCount")]
    active_count: i64,
    edges: BTreeSet<i64>,
    #[serde(rename = "waveByNode")]
    wave_by_node: BTreeMap<i64, i64>,
    declaration: Vec<i64>,
    completion: Vec<i64>,
    #[serde(rename = "responseOrdinals")]
    response_ordinals: Vec<i64>,
    #[serde(rename = "plannedReads")]
    planned_reads: i64,
    #[serde(rename = "reservedReads")]
    reserved_reads: i64,
    #[serde(rename = "attemptVersion")]
    attempt_version: i64,
    #[serde(rename = "publishedVersion")]
    published_version: i64,
    published: bool,
    failed: bool,
    #[serde(rename = "selectedMappedEdges")]
    selected_mapped_edges: BTreeSet<i64>,
    strategy: String,
    outcome: String,
}

impl ReadSequenceDagState {
    fn initial() -> Self {
        Self {
            active_count: NODE_COUNT as i64,
            edges: [2, 12, 23, 24, 35].into_iter().collect(),
            wave_by_node: BTreeMap::from([(0, 0), (1, 0), (2, 1), (3, 2), (4, 2), (5, 3)]),
            declaration: (0..NODE_COUNT as i64).collect(),
            completion: Vec::new(),
            response_ordinals: Vec::new(),
            planned_reads: 0,
            reserved_reads: 0,
            attempt_version: 0,
            published_version: 0,
            published: false,
            failed: false,
            selected_mapped_edges: BTreeSet::new(),
            strategy: "ordinary".to_string(),
            outcome: "ready".to_string(),
        }
    }
}

impl State<ReadSequenceDagDriver> for ReadSequenceDagState {
    fn from_driver(driver: &ReadSequenceDagDriver) -> Result<Self> {
        Ok(driver.state.clone())
    }
}

#[derive(Debug)]
struct ReadSequenceDagDriver {
    state: ReadSequenceDagState,
}

impl Default for ReadSequenceDagDriver {
    fn default() -> Self {
        Self {
            state: ReadSequenceDagState::initial(),
        }
    }
}

impl Driver for ReadSequenceDagDriver {
    type State = ReadSequenceDagState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            LaterDeclaration => {
                self.replay_graph(&[2, 0, 1, 4, 3, 5], false)?;
                self.state = ReadSequenceDagState::initial();
                self.state.declaration = vec![2, 0, 1, 4, 3, 5];
                self.state.completion = vec![1, 0, 2, 4, 3, 5];
                self.state.response_ordinals = (0..NODE_COUNT as i64).collect();
                self.state.planned_reads = 6;
                self.state.reserved_reads = 6;
                self.state.attempt_version = 11;
                self.state.published_version = 11;
                self.state.published = true;
                self.state.outcome = "accepted".to_string();
            },
            MultipleParents => {
                self.replay_multiple_parent_graph()?;
                self.state = ReadSequenceDagState::initial();
                self.state.edges = [2, 12, 24].into_iter().collect();
                self.state.wave_by_node = BTreeMap::from([(0, 0), (1, 0), (2, 1), (3, 0), (4, 2), (5, 0)]);
                self.state.planned_reads = 4;
                self.state.reserved_reads = 4;
                self.state.outcome = "accepted".to_string();
            },
            RetryAfterFailure => {
                self.replay_graph(&(0..NODE_COUNT as i64).collect::<Vec<_>>(), false)?;
                self.state = ReadSequenceDagState::initial();
                self.state.completion = vec![1, 0, 2, 3, 4, 5];
                self.state.response_ordinals = (0..NODE_COUNT as i64).collect();
                self.state.planned_reads = 6;
                self.state.reserved_reads = 6;
                self.state.attempt_version = 22;
                self.state.published_version = 22;
                self.state.published = true;
                self.state.outcome = "accepted_after_retry".to_string();
            },
            MappedSelection => {
                self.replay_graph(&(0..NODE_COUNT as i64).collect::<Vec<_>>(), true)?;
                let selected = self.mapped_selection()?;
                anyhow::ensure!(
                    selected == BTreeSet::from([24]),
                    "mapped edge replay selected {selected:?}"
                );
                self.state = ReadSequenceDagState::initial();
                self.state.selected_mapped_edges = selected;
                self.state.strategy = "mapped".to_string();
                self.state.planned_reads = 5;
                self.state.reserved_reads = 5;
                self.state.outcome = "accepted".to_string();
            },
            SqlFallback => {
                self.replay_graph(&(0..NODE_COUNT as i64).collect::<Vec<_>>(), false)?;
                self.state = ReadSequenceDagState::initial();
                self.state.strategy = "fallback".to_string();
                self.state.planned_reads = 6;
                self.state.reserved_reads = 6;
                self.state.outcome = "accepted".to_string();
            },
            step => {},
        })
    }
}

impl ReadSequenceDagDriver {
    fn replay_graph(&self, declaration: &[i64], mapped_input: bool) -> Result<ReadSequencePlan> {
        let request = graph_request(declaration, mapped_input);
        let plan = plan_read_sequence(&request)?;
        let (edges, waves) = normalized_graph(&plan)?;
        anyhow::ensure!(edges == NORMAL_EDGES.iter().copied().collect());
        anyhow::ensure!(waves == BTreeMap::from([(0, 0), (1, 0), (2, 1), (3, 2), (4, 2), (5, 3)]));
        Ok(plan)
    }

    fn replay_multiple_parent_graph(&self) -> Result<ReadSequencePlan> {
        let plan = plan_read_sequence(&multiple_parent_request())?;
        let (edges, waves) = normalized_graph(&plan)?;
        anyhow::ensure!(edges == [2, 12, 24].into_iter().collect());
        anyhow::ensure!(waves == BTreeMap::from([(0, 0), (1, 0), (2, 1), (3, 0), (4, 2), (5, 0)]));
        Ok(plan)
    }

    fn mapped_selection(&self) -> Result<BTreeSet<i64>> {
        let plan = self.replay_graph(&(0..NODE_COUNT as i64).collect::<Vec<_>>(), true)?;
        let descriptors = [
            (
                ReadSequenceNodeId::from_index(2),
                ReadSequencePhysicalDescriptor {
                    operation: ReadSequencePhysicalOperation::PrefixRange,
                    tuple_schema: true,
                    tuple_prefix_safe: true,
                    selector_physical: true,
                    ..Default::default()
                },
            ),
            (
                ReadSequenceNodeId::from_index(4),
                ReadSequencePhysicalDescriptor {
                    operation: ReadSequencePhysicalOperation::Point,
                    tuple_schema: true,
                    tuple_prefix_safe: true,
                    selector_physical: true,
                    ..Default::default()
                },
            ),
        ];
        Ok(select_read_sequence_mapped_edges(
            &plan,
            &descriptors,
            ReadSequenceMappedOptions {
                foundationdb: true,
                api_version: 740,
                enabled: true,
                consistency: ReadSequenceConsistency::Eventual,
            },
        )
        .selected
        .into_iter()
        .map(|edge| edge.parent.index() as i64 * 10 + edge.child.index() as i64)
        .collect())
    }
}

fn get(name: &str) -> ReadSequenceNode {
    ReadSequenceNode {
        name: name.to_string(),
        operation: ReadSequenceNodeOperation::Get(GetItemRequest::new(
            TableName::new("items"),
            KeyAttributes::from([(String::from("id"), AttributeValue::S(name.to_string()))]),
        )),
        inputs: None,
        iterate: None,
        after: None,
    }
}

fn graph_request(declaration: &[i64], mapped_input: bool) -> ReadSequenceRequest {
    let mut nodes = declaration
        .iter()
        .map(|index| get(&format!("node{index}")))
        .collect::<Vec<_>>();
    let by_name = |index: usize| format!("node{index}");
    for (index, after) in [(2, vec![0, 1]), (3, vec![2]), (4, vec![2]), (5, vec![3])] {
        let node = nodes
            .iter_mut()
            .find(|node| node.name == by_name(index))
            .expect("declared node");
        node.after = Some(after.into_iter().map(by_name).collect::<Vec<_>>());
    }
    if mapped_input {
        let node = nodes
            .iter_mut()
            .find(|node| node.name == "node4")
            .expect("mapped child");
        node.operation = ReadSequenceNodeOperation::Get(GetItemRequest::new(
            TableName::new("items"),
            [("id".to_string(), read_sequence_input_marker("id"))]
                .into_iter()
                .collect::<KeyAttributes>(),
        ));
        node.inputs = Some(
            [(
                "id".to_string(),
                ReadSequenceNodeInput {
                    from: ReadSequenceFromInput {
                        node: "node2".to_string(),
                        select: ReadSequenceSelector("$.Query.Items[*].id".to_string()),
                    },
                    mapped_key_source: None,
                    cardinality: ReadSequenceInputCardinality::Many,
                    on_missing: ReadSequenceOnMissing::Skip,
                },
            )]
            .into_iter()
            .collect(),
        );
        node.iterate = Some("id".to_string());
    }
    if let Some(node) = nodes.iter_mut().find(|node| node.name == "node2") {
        node.operation = ReadSequenceNodeOperation::Query(QueryRequest {
            table_name: TableName::new("items"),
            key_condition_expression: "pk = :pk".to_string(),
            expression_attribute_values: Some(
                [(":pk".to_string(), AttributeValue::S("x".to_string()))]
                    .into_iter()
                    .collect(),
            ),
            ..QueryRequest::new(TableName::new("items"), "pk = :pk".to_string())
        });
    }
    ReadSequenceRequest::new(nodes)
}

fn multiple_parent_request() -> ReadSequenceRequest {
    let mut nodes = (0..NODE_COUNT)
        .map(|index| get(&format!("node{index}")))
        .collect::<Vec<_>>();
    nodes[2].after = Some(vec!["node0".to_string(), "node1".to_string()]);
    nodes[4].after = Some(vec!["node2".to_string()]);
    ReadSequenceRequest::new(nodes)
}

fn normalized_graph(plan: &ReadSequencePlan) -> Result<(BTreeSet<i64>, BTreeMap<i64, i64>)> {
    let node_id = |id: ReadSequenceNodeId| -> Result<i64> {
        plan.graph
            .node_name(id)
            .and_then(|name| name.strip_prefix("node"))
            .ok_or_else(|| anyhow::anyhow!("invalid node name"))?
            .parse::<i64>()
            .map_err(Into::into)
    };
    let edges = plan.graph.dependencies.iter().enumerate().try_fold(
        BTreeSet::new(),
        |mut edges, (child, parents)| {
            let child = node_id(ReadSequenceNodeId::from_index(child))?;
            for parent in parents {
                edges.insert(node_id(*parent)? * 10 + child);
            }
            Ok::<_, anyhow::Error>(edges)
        },
    )?;
    let waves = plan.graph.waves.iter().enumerate().try_fold(
        BTreeMap::new(),
        |mut waves, (wave, nodes)| {
            for node in nodes {
                waves.insert(node_id(*node)?, wave as i64);
            }
            Ok::<_, anyhow::Error>(waves)
        },
    )?;
    Ok((edges, waves))
}

#[quint_run(
    spec = "../../quint/read_sequence_dag.qnt",
    max_samples = 200,
    max_steps = 8,
    seed = "0x71ead"
)]
fn read_sequence_dag_mbt_replays_production_planner() -> impl Driver {
    ReadSequenceDagDriver::default()
}
