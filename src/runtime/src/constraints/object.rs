use std::collections::HashMap;

use crate::{InstancePath, LinkMLInstance, ValidationIssueCode, ValidationIssueSink};
use linkml_schemaview::schemaview::ClassView;

pub struct ObjectConstraintContext<'a> {
    pub class: &'a ClassView,
    pub values: &'a HashMap<String, LinkMLInstance>,
    #[allow(dead_code)]
    pub unknown_fields: &'a HashMap<String, serde_json::Value>,
    pub path: InstancePath,
}

pub trait ObjectConstraint: Send + Sync {
    fn evaluate(&self, ctx: &ObjectConstraintContext, sink: &mut ValidationIssueSink);
}

struct RequiredSlotConstraint;

impl ObjectConstraint for RequiredSlotConstraint {
    fn evaluate(&self, ctx: &ObjectConstraintContext, sink: &mut ValidationIssueSink) {
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
                    ValidationIssueCode::MissingRequiredSlot,
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
    fn evaluate(&self, ctx: &ObjectConstraintContext, sink: &mut ValidationIssueSink) {
        for (extra, _) in ctx.unknown_fields.iter() {
            let mut path = ctx.path.clone();
            path.push(extra.clone());
            sink.push_error(
                ValidationIssueCode::UnknownSlot,
                path,
                format!("unknown slot '{}' for class '{}'", extra, ctx.class.name()),
            );
        }
    }
}

struct UnknownDeclaredSlotConstraint;

impl ObjectConstraint for UnknownDeclaredSlotConstraint {
    fn evaluate(&self, ctx: &ObjectConstraintContext, sink: &mut ValidationIssueSink) {
        for (key, _) in ctx.values.iter() {
            let known = ctx.class.slots().iter().any(|s| s.name == *key);
            if !known {
                let mut path = ctx.path.clone();
                path.push(key.clone());
                sink.push_error(
                    ValidationIssueCode::UnknownSlot,
                    path,
                    format!("unknown slot '{}' for class '{}'", key, ctx.class.name()),
                );
            }
        }
    }
}

static OBJECT_CONSTRAINTS: &[&dyn ObjectConstraint] = &[
    &RequiredSlotConstraint,
    &UnknownFieldConstraint,
    &UnknownDeclaredSlotConstraint,
];

pub fn run_object_constraints(
    class: &ClassView,
    values: &HashMap<String, LinkMLInstance>,
    unknown_fields: &HashMap<String, serde_json::Value>,
    path: InstancePath,
    sink: &mut ValidationIssueSink,
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
