use std::collections::HashMap;

use crate::{InstancePath, LinkMLInstance, ValidationProblemType, ValidationResultSink};
use linkml_schemaview::schemaview::ClassView;

pub struct ObjectConstraintContext<'a> {
    pub class: &'a ClassView,
    pub values: &'a HashMap<String, LinkMLInstance>,
    #[allow(dead_code)]
    pub unknown_fields: &'a HashMap<String, serde_json::Value>,
    pub path: InstancePath,
}

pub trait ObjectConstraint: Send + Sync {
    fn evaluate(&self, ctx: &ObjectConstraintContext, sink: &mut ValidationResultSink);
}

struct RequiredSlotConstraint;

impl ObjectConstraint for RequiredSlotConstraint {
    fn evaluate(&self, ctx: &ObjectConstraintContext, sink: &mut ValidationResultSink) {
        for slot in ctx.class.slots() {
            let def = slot.definition();
            let required = def.required.unwrap_or(false)
                || def.minimum_cardinality.map(|min| min > 0).unwrap_or(false);
            if !required {
                continue;
            }
            let key = &slot.name;
            let missing = match ctx.values.get(key) {
                None => true,
                Some(LinkMLInstance::Null { .. }) => true,
                Some(LinkMLInstance::List { values, .. }) => values.is_empty(),
                Some(LinkMLInstance::Mapping { values, .. }) => values.is_empty(),
                _ => false,
            };
            if missing {
                let mut path = ctx.path.clone();
                path.push(key.clone());
                sink.push_error(
                    ValidationProblemType::MissingSlotValue,
                    path,
                    format!(
                        "required slot '{}' missing for class '{}'",
                        key,
                        ctx.class.name()
                    ),
                );
            }
        }
    }
}

struct UnknownFieldConstraint;

impl ObjectConstraint for UnknownFieldConstraint {
    fn evaluate(&self, ctx: &ObjectConstraintContext, sink: &mut ValidationResultSink) {
        for (extra, _) in ctx.unknown_fields.iter() {
            let mut path = ctx.path.clone();
            path.push(extra.clone());
            sink.push_error(
                ValidationProblemType::UndeclaredSlot,
                path,
                format!("unknown slot '{}' for class '{}'", extra, ctx.class.name()),
            );
        }
    }
}

struct UnknownDeclaredSlotConstraint;

impl ObjectConstraint for UnknownDeclaredSlotConstraint {
    fn evaluate(&self, ctx: &ObjectConstraintContext, sink: &mut ValidationResultSink) {
        for (key, _) in ctx.values.iter() {
            let known = ctx.class.slots().iter().any(|s| s.name == *key);
            if !known {
                let mut path = ctx.path.clone();
                path.push(key.clone());
                sink.push_error(
                    ValidationProblemType::UndeclaredSlot,
                    path,
                    format!("unknown slot '{}' for class '{}'", key, ctx.class.name()),
                );
            }
        }
    }
}

struct CardinalityConstraint;

impl ObjectConstraint for CardinalityConstraint {
    fn evaluate(&self, ctx: &ObjectConstraintContext, sink: &mut ValidationResultSink) {
        for slot in ctx.class.slots() {
            let def = slot.definition();
            let exact = def.exact_cardinality;
            let min = def.minimum_cardinality;
            let max = def.maximum_cardinality;
            if exact.is_none() && min.is_none() && max.is_none() {
                continue;
            }
            let count = ctx
                .values
                .get(&slot.name)
                .map(slot_value_count)
                .unwrap_or(0);
            let mut path = ctx.path.clone();
            path.push(slot.name.clone());
            if let Some(expected) = exact {
                if (count as isize) != expected {
                    sink.push_error(
                        ValidationProblemType::MaxCountViolation,
                        path,
                        format!(
                            "slot '{}' in class '{}' requires exactly {} value(s); found {}",
                            slot.name,
                            ctx.class.name(),
                            expected,
                            count
                        ),
                    );
                }
                continue;
            }
            if let Some(minimum) = min {
                if (count as isize) < minimum {
                    sink.push_error(
                        ValidationProblemType::MissingSlotValue,
                        path.clone(),
                        format!(
                            "slot '{}' in class '{}' requires at least {} value(s); found {}",
                            slot.name,
                            ctx.class.name(),
                            minimum,
                            count
                        ),
                    );
                }
            }
            if let Some(maximum) = max {
                if (count as isize) > maximum {
                    sink.push_error(
                        ValidationProblemType::MaxCountViolation,
                        path.clone(),
                        format!(
                            "slot '{}' in class '{}' allows at most {} value(s); found {}",
                            slot.name,
                            ctx.class.name(),
                            maximum,
                            count
                        ),
                    );
                }
            }
        }
    }
}

static OBJECT_CONSTRAINTS: &[&dyn ObjectConstraint] = &[
    &RequiredSlotConstraint,
    &UnknownFieldConstraint,
    &UnknownDeclaredSlotConstraint,
    &CardinalityConstraint,
];

pub fn run_object_constraints(
    class: &ClassView,
    values: &HashMap<String, LinkMLInstance>,
    unknown_fields: &HashMap<String, serde_json::Value>,
    path: InstancePath,
    sink: &mut ValidationResultSink,
) {
    let ctx = ObjectConstraintContext {
        class,
        values,
        unknown_fields,
        path,
    };
    for constraint in OBJECT_CONSTRAINTS {
        constraint.evaluate(&ctx, sink);
    }
}

fn slot_value_count(value: &LinkMLInstance) -> usize {
    match value {
        LinkMLInstance::Null { .. } => 0,
        LinkMLInstance::List { values, .. } => values
            .iter()
            .filter(|child| !matches!(child, LinkMLInstance::Null { .. }))
            .count(),
        LinkMLInstance::Mapping { values, .. } => values
            .values()
            .filter(|child| !matches!(child, LinkMLInstance::Null { .. }))
            .count(),
        _ => 1,
    }
}
