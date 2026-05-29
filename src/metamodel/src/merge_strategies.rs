//! Merge strategies for use with `merge_derive`.

use std::collections::HashMap;
use std::hash::Hash;

/// Merge two `Option<HashMap<K, V>>`s per-key: right wins on a key conflict,
/// keys only present on the left are preserved. `None` on the right is a no-op.
pub fn option_map_overwrite<K: Eq + Hash, V>(
    left: &mut Option<HashMap<K, V>>,
    right: Option<HashMap<K, V>>,
) {
    match (left.as_mut(), right) {
        (Some(l), Some(r)) => l.extend(r),
        (None, r @ Some(_)) => *left = r,
        _ => {}
    }
}
