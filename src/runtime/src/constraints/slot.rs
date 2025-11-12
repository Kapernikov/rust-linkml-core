use std::sync::Mutex;

use once_cell::sync::Lazy;
use regex::Regex;

use crate::{DiagnosticCode, DiagnosticSink, InstancePath};
use linkml_schemaview::schemaview::{ClassView, SlotView};
use serde_json::Value as JsonValue;

pub struct SlotConstraintContext<'a> {
    #[allow(dead_code)]
    pub class: Option<&'a ClassView>,
    pub slot: &'a SlotView,
    pub value: &'a JsonValue,
    pub path: InstancePath,
}

pub trait SlotConstraint: Send + Sync {
    fn applies(&self, ctx: &SlotConstraintContext) -> bool;
    fn evaluate(&self, ctx: &SlotConstraintContext, sink: &mut DiagnosticSink);
}

static SLOT_CONSTRAINTS: &[&dyn SlotConstraint] = &[&EnumConstraint, &RegexConstraint];

pub fn run_slot_constraints(
    class: Option<&ClassView>,
    slot: &SlotView,
    value: &JsonValue,
    path: InstancePath,
    sink: &mut DiagnosticSink,
) {
    let ctx = SlotConstraintContext {
        class,
        slot,
        value,
        path,
    };
    for constraint in SLOT_CONSTRAINTS {
        if constraint.applies(&ctx) {
            constraint.evaluate(&ctx, sink);
        }
    }
}

struct EnumConstraint;

impl SlotConstraint for EnumConstraint {
    fn applies(&self, ctx: &SlotConstraintContext) -> bool {
        ctx.slot.get_range_enum().is_some() && !ctx.value.is_null()
    }

    fn evaluate(&self, ctx: &SlotConstraintContext, sink: &mut DiagnosticSink) {
        let Some(enum_view) = ctx.slot.get_range_enum() else {
            return;
        };
        let JsonValue::String(s) = ctx.value else {
            sink.push_error(
                DiagnosticCode::InvalidEnumValue,
                ctx.path.clone(),
                format!(
                    "expected string for enum '{}' in slot '{}'",
                    enum_view.name(),
                    ctx.slot.name
                ),
            );
            return;
        };
        match enum_view.permissible_value_keys() {
            Ok(keys) => {
                if !keys.contains(s) {
                    sink.push_error(
                        DiagnosticCode::InvalidEnumValue,
                        ctx.path.clone(),
                        format!(
                            "invalid enum value '{}' for slot '{}' (allowed: {:?})",
                            s, ctx.slot.name, keys
                        ),
                    );
                }
            }
            Err(err) => sink.push_error(
                DiagnosticCode::InvalidEnumValue,
                ctx.path.clone(),
                format!("failed to resolve enum '{}': {:?}", enum_view.name(), err),
            ),
        }
    }
}

struct RegexConstraint;

static REGEX_CACHE: Lazy<Mutex<std::collections::HashMap<String, Regex>>> =
    Lazy::new(|| Mutex::new(std::collections::HashMap::new()));

fn compile_regex(pattern: &str) -> Option<Regex> {
    let mut cache = REGEX_CACHE.lock().expect("regex cache poisoned");
    if let Some(existing) = cache.get(pattern) {
        return Some(existing.clone());
    }
    match Regex::new(pattern) {
        Ok(r) => {
            cache.insert(pattern.to_string(), r.clone());
            Some(r)
        }
        Err(_) => None,
    }
}

impl SlotConstraint for RegexConstraint {
    fn applies(&self, ctx: &SlotConstraintContext) -> bool {
        ctx.slot
            .definition()
            .pattern
            .as_ref()
            .map(|p| !p.is_empty())
            .unwrap_or(false)
            && !ctx.value.is_null()
    }

    fn evaluate(&self, ctx: &SlotConstraintContext, sink: &mut DiagnosticSink) {
        let Some(pattern) = ctx.slot.definition().pattern.as_ref() else {
            return;
        };
        let Some(regex) = compile_regex(pattern) else {
            sink.push_error(
                DiagnosticCode::RegexCompileError,
                ctx.path.clone(),
                format!("invalid regex '{}' for slot '{}'", pattern, ctx.slot.name),
            );
            return;
        };
        let JsonValue::String(s) = ctx.value else {
            sink.push_error(
                DiagnosticCode::RegexMismatch,
                ctx.path.clone(),
                format!(
                    "pattern '{}' requires string value in slot '{}'",
                    pattern, ctx.slot.name
                ),
            );
            return;
        };
        if !regex.is_match(s) {
            sink.push_error(
                DiagnosticCode::RegexMismatch,
                ctx.path.clone(),
                format!(
                    "value '{}' does not match pattern '{}' for slot '{}'",
                    s, pattern, ctx.slot.name
                ),
            );
        }
    }
}
