use std::sync::Mutex;

use once_cell::sync::Lazy;
use regex::Regex;

use crate::{InstancePath, ValidationProblemType, ValidationResultSink};
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
    fn evaluate(&self, ctx: &SlotConstraintContext, sink: &mut ValidationResultSink);
}

static SLOT_CONSTRAINTS: &[&dyn SlotConstraint] =
    &[&EnumConstraint, &RegexConstraint, &RangeConstraint];

pub fn run_slot_constraints(
    class: Option<&ClassView>,
    slot: &SlotView,
    value: &JsonValue,
    path: InstancePath,
    sink: &mut ValidationResultSink,
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

    fn evaluate(&self, ctx: &SlotConstraintContext, sink: &mut ValidationResultSink) {
        let Some(enum_view) = ctx.slot.get_range_enum() else {
            return;
        };
        let JsonValue::String(s) = ctx.value else {
            sink.push_error(
                ValidationProblemType::SlotRangeViolation,
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
                        ValidationProblemType::SlotRangeViolation,
                        ctx.path.clone(),
                        format!(
                            "invalid enum value '{}' for slot '{}' (allowed: {:?})",
                            s, ctx.slot.name, keys
                        ),
                    );
                }
            }
            Err(err) => sink.push_error(
                ValidationProblemType::SlotRangeViolation,
                ctx.path.clone(),
                format!("failed to resolve enum '{}': {:?}", enum_view.name(), err),
            ),
        }
    }
}

struct RegexConstraint;

static REGEX_CACHE: Lazy<Mutex<std::collections::HashMap<String, Regex>>> =
    Lazy::new(|| Mutex::new(std::collections::HashMap::new()));

fn cache_lock() -> std::sync::MutexGuard<'static, std::collections::HashMap<String, Regex>> {
    match REGEX_CACHE.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

struct RangeConstraint;

impl SlotConstraint for RangeConstraint {
    fn applies(&self, ctx: &SlotConstraintContext) -> bool {
        let def = ctx.slot.definition();
        (def.minimum_value.is_some() || def.maximum_value.is_some()) && !ctx.value.is_null()
    }

    fn evaluate(&self, ctx: &SlotConstraintContext, sink: &mut ValidationResultSink) {
        let value = match numeric_value(ctx.value) {
            Some(v) => v,
            None => return,
        };
        let def = ctx.slot.definition();
        if let Some(minimum) = def.minimum_value.as_ref().and_then(anything_to_f64) {
            if value < minimum {
                sink.push_error(
                    ValidationProblemType::SlotRangeViolation,
                    ctx.path.clone(),
                    format!(
                        "value {} is below minimum {} for slot '{}'",
                        value, minimum, ctx.slot.name
                    ),
                );
            }
        }
        if let Some(maximum) = def.maximum_value.as_ref().and_then(anything_to_f64) {
            if value > maximum {
                sink.push_error(
                    ValidationProblemType::SlotRangeViolation,
                    ctx.path.clone(),
                    format!(
                        "value {} exceeds maximum {} for slot '{}'",
                        value, maximum, ctx.slot.name
                    ),
                );
            }
        }
    }
}

fn numeric_value(value: &JsonValue) -> Option<f64> {
    match value {
        JsonValue::Number(n) => n.as_f64(),
        JsonValue::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn anything_to_f64(any: &linkml_meta::Anything) -> Option<f64> {
    serde_json::to_value(any).ok().and_then(|v| match v {
        JsonValue::Number(n) => n.as_f64(),
        JsonValue::String(s) => s.parse::<f64>().ok(),
        JsonValue::Bool(b) => Some(if b { 1.0 } else { 0.0 }),
        _ => None,
    })
}

fn compile_regex(pattern: &str) -> Option<Regex> {
    let mut cache = cache_lock();
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

    fn evaluate(&self, ctx: &SlotConstraintContext, sink: &mut ValidationResultSink) {
        let Some(pattern) = ctx.slot.definition().pattern.as_ref() else {
            return;
        };
        let Some(regex) = compile_regex(pattern) else {
            sink.push_error(
                ValidationProblemType::ParsingError,
                ctx.path.clone(),
                format!("invalid regex '{}' for slot '{}'", pattern, ctx.slot.name),
            );
            return;
        };
        let JsonValue::String(s) = ctx.value else {
            sink.push_error(
                ValidationProblemType::SlotRangeViolation,
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
                ValidationProblemType::SlotRangeViolation,
                ctx.path.clone(),
                format!(
                    "value '{}' does not match pattern '{}' for slot '{}'",
                    s, pattern, ctx.slot.name
                ),
            );
        }
    }
}
