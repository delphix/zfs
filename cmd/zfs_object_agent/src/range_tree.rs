use more_asserts::*;
use std::collections::BTreeMap;

#[derive(Default)]
pub struct RangeTree {
    tree: BTreeMap<u64, u64>, // start -> size
}

impl RangeTree {
    pub fn new() -> RangeTree {
        RangeTree {
            tree: BTreeMap::new(),
        }
    }

    // panic if already present
    pub fn add(&mut self, start: u64, size: u64) {
        if size == 0 {
            return;
        }

        let end = start + size;
        let before = self.tree.range(..end).next_back();
        let after = self.tree.range(start..).next();

        let merge_before = match before {
            Some((before_start, before_size)) => {
                assert_le!(before_start + before_size, start);
                *before_start + *before_size == start
            }
            None => false,
        };

        let merge_after = match after {
            Some((after_start, _after_size)) => {
                assert_ge!(*after_start, end);
                *after_start == end
            }
            None => false,
        };

        if merge_before && merge_after {
            let before_start = *before.unwrap().0;
            let after_start = *after.unwrap().0;
            self.tree
                .entry(before_start)
                .and_modify(|before_size| *before_size += size);
            self.tree.remove(&after_start);
        } else if merge_before {
            let before_start = *before.unwrap().0;
            self.tree
                .entry(before_start)
                .and_modify(|before_size| *before_size += size);
        } else if merge_after {
            let after_start = *after.unwrap().0;
            let after_size = *after.unwrap().1;
            self.tree.remove(&after_start);
            self.tree.insert(start, size + after_size);
        } else {
            self.tree.insert(start, size);
        }
    }

    // panic if not present
    pub fn remove(&mut self, start: u64, size: u64) {
        assert_ne!(size, 0);

        let end = start + size;
        let (existing_start_ref, existing_size_ref) =
            self.tree.range_mut(..end).next_back().unwrap();
        let existing_start = *existing_start_ref;
        let existing_size = *existing_size_ref;
        let existing_end = existing_start + existing_size;
        assert_le!(existing_start, start);
        assert_ge!(existing_end, end);
        let left_over = existing_start != start;
        let right_over = existing_end != end;

        if left_over && right_over {
            *existing_size_ref = start - existing_start;
            self.tree.insert(end, existing_end - end);
        } else if left_over {
            *existing_size_ref = start - existing_start;
        } else if right_over {
            self.tree.remove(&start);
            self.tree.insert(end, existing_end - end);
        } else {
            self.tree.remove(&start);
        }
    }

    /// Returns Iter<start, size>
    pub fn iter(&self) -> std::collections::btree_map::Iter<u64, u64> {
        self.tree.iter()
    }

    pub fn clear(&mut self) {
        self.tree.clear();
    }
}
