use std::collections::HashSet;

use crate::classview::ClassView;
use crate::identifier::Identifier;
use crate::schemaview::{SchemaView, SchemaViewError};
use crate::slotview::SlotView;

impl SchemaView {
    /// Resolve a slot path starting from `class_id`, returning every [`SlotView`]
    /// reachable at the terminal segment.
    ///
    /// The `path` must contain slot names (no object data paths), and each step
    /// is matched against the slots visible on the current class context. When
    /// a segment fans out (e.g. due to unions, mixins, or subclass overrides),
    /// the traversal keeps every branch so ambiguity is preserved in the final
    /// result.
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
        let mut current_classes = match self.get_class(class_id, &conv)? {
            Some(cv) => vec![cv],
            None => return Err(SchemaViewError::NotFound),
        };

        let mut terminal_matches: Vec<SlotView> = Vec::new();

        while let Some(segment) = segments.next() {
            let (segment_matches, next_classes) =
                Self::collect_segment_matches(&current_classes, segment);

            if segment_matches.is_empty() {
                return Ok(Vec::new());
            }

            terminal_matches = segment_matches;

            if segments.peek().is_some() {
                if next_classes.is_empty() {
                    return Ok(Vec::new());
                }
                current_classes = next_classes;
            }
        }

        Ok(terminal_matches)
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

                for range_class in slot
                    .get_range_info()
                    .iter()
                    .filter_map(|ri| ri.range_class.clone())
                {
                    let key = (
                        range_class.schema_id().to_string(),
                        range_class.name().to_string(),
                    );
                    if seen_next.insert(key) {
                        next_classes.push(range_class);
                    }
                }
            }
        }

        (matches, next_classes)
    }
}
