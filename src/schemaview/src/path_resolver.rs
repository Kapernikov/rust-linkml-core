use std::collections::HashSet;

use crate::classview::ClassView;
use crate::identifier::Identifier;
use crate::schemaview::{SchemaView, SchemaViewError};
use crate::slotview::{SlotContainerMode, SlotInlineMode, SlotView};

impl SchemaView {
    /// Resolve a slot path starting from `class_id`, returning every [`SlotView`]
    /// reachable at the terminal segment.
    ///
    /// The path may contain:
    /// - Slot names matched against the current class context (and subclasses)
    /// - List indices (numeric like "0" or identifier-based like "person_id")
    /// - Mapping keys (any string) for slots with `inlined: true` and a key slot
    ///
    /// When a segment fans out (e.g. due to unions, mixins, or subclass overrides),
    /// the traversal keeps every branch so ambiguity is preserved in the final
    /// result.
    ///
    /// Note: Non-inlined (reference) slots are not traversed since they only
    /// contain foreign keys, not embedded data.
    pub fn slots_for_path<'a, I>(
        &self,
        class_id: &Identifier,
        path: I,
    ) -> Result<Vec<SlotView>, SchemaViewError>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let mut segments = path.into_iter().peekable();
        if segments.peek().is_none() {
            return Ok(Vec::new());
        }

        let conv = self.converter();
        let base_class = match self.get_class(class_id, &conv)? {
            Some(cv) => cv,
            None => return Err(SchemaViewError::NotFound),
        };

        // Include the base class and all its subclasses for initial slot matching
        let mut current_classes = Self::expand_with_descendants(&[base_class]);

        let mut terminal_matches: Vec<SlotView> = Vec::new();

        while let Some(segment) = segments.next() {
            let (segment_matches, next_classes) =
                Self::collect_segment_matches(&current_classes, segment);

            if segment_matches.is_empty() {
                // No slot matched - path invalid
                return Ok(Vec::new());
            }

            terminal_matches = segment_matches.clone();

            // Check if the matched slots have indexed container modes (List or Mapping)
            let requires_index = Self::slots_require_index(&segment_matches);

            if let Some(next_segment) = segments.peek() {
                if next_classes.is_empty() {
                    // Scalar slot with no range class - if requires index, consume it
                    if requires_index {
                        segments.next(); // consume the index segment
                                         // No more traversal possible after scalar list
                    }
                    // If there are still segments after this, path is invalid
                    if segments.peek().is_some() {
                        return Ok(Vec::new());
                    }
                } else if requires_index {
                    // Consume the index/key segment - it's not a slot name
                    // First check if it could also be a valid slot name (ambiguity)
                    let (potential_slot_matches, _) =
                        Self::collect_segment_matches(&next_classes, next_segment);

                    if potential_slot_matches.is_empty() {
                        // Not a slot name, must be an index/key - skip it
                        segments.next();
                    }
                    // If it IS a valid slot name, don't consume - let next iteration handle it
                    // (This handles cases where someone writes ["items", "name"] instead of
                    // ["items", "0", "name"] - we try slot name first)

                    current_classes = next_classes;
                } else {
                    current_classes = next_classes;
                }
            }
        }

        Ok(terminal_matches)
    }

    /// Check if any of the matched slots have List or Mapping container mode,
    /// meaning the next path segment should be treated as an index/key.
    fn slots_require_index(slots: &[SlotView]) -> bool {
        slots.iter().any(|slot| {
            slot.get_range_info().iter().any(|ri| {
                matches!(
                    ri.slot_container_mode,
                    SlotContainerMode::List | SlotContainerMode::Mapping
                )
            })
        })
    }

    /// Expand a list of classes to include all their descendants (subclasses).
    fn expand_with_descendants(classes: &[ClassView]) -> Vec<ClassView> {
        let mut result = Vec::new();
        let mut seen = HashSet::new();

        for class in classes {
            let key = (class.schema_id().to_string(), class.name().to_string());
            if seen.insert(key) {
                result.push(class.clone());
            }

            if let Ok(descendants) = class.get_descendants(true, true) {
                for descendant in descendants {
                    let desc_key = (
                        descendant.schema_id().to_string(),
                        descendant.name().to_string(),
                    );
                    if seen.insert(desc_key) {
                        result.push(descendant);
                    }
                }
            }
        }

        result
    }

    fn collect_segment_matches(
        classes: &[ClassView],
        segment: &str,
    ) -> (Vec<SlotView>, Vec<ClassView>) {
        let mut matches = Vec::new();
        let mut next_classes = Vec::new();
        let mut seen_next = HashSet::new();

        for class_view in classes {
            for slot in class_view
                .slots()
                .iter()
                .filter(|slot| slot.name == segment)
            {
                matches.push(slot.clone());

                for ri in slot.get_range_info().iter() {
                    // Skip non-inlined references - they're just foreign keys,
                    // not embedded data that can be traversed
                    if ri.slot_inline_mode == SlotInlineMode::Reference {
                        continue;
                    }

                    if let Some(range_class) = ri.range_class.clone() {
                        // Add the range class itself
                        let key = (
                            range_class.schema_id().to_string(),
                            range_class.name().to_string(),
                        );
                        if seen_next.insert(key) {
                            next_classes.push(range_class.clone());
                        }

                        // Also add all subclasses (descendants) of the range class
                        // This allows paths like ["locations", "0", "SpotLocation_coordinates"]
                        // where locations has range BaseLocation but SpotLocation inherits from it
                        if let Ok(descendants) = range_class.get_descendants(true, true) {
                            for descendant in descendants {
                                let desc_key = (
                                    descendant.schema_id().to_string(),
                                    descendant.name().to_string(),
                                );
                                if seen_next.insert(desc_key) {
                                    next_classes.push(descendant);
                                }
                            }
                        }
                    }
                }
            }
        }

        (matches, next_classes)
    }
}
