use crate::base_types::*;
use more_asserts::*;
use std::borrow::Borrow;
use std::collections::BTreeSet;
use std::ops::Bound::*;

#[derive(Debug)]
pub struct ObjectBlockMap {
    map: BTreeSet<ObjectBlockMapEntry>,
}

#[derive(Debug, Ord, PartialOrd, PartialEq, Eq, Copy, Clone)]
pub struct ObjectBlockMapEntry {
    pub object: ObjectID,
    pub block: BlockID,
}

impl Borrow<ObjectID> for ObjectBlockMapEntry {
    fn borrow(&self) -> &ObjectID {
        &self.object
    }
}

impl Borrow<BlockID> for ObjectBlockMapEntry {
    fn borrow(&self) -> &BlockID {
        &self.block
    }
}

impl ObjectBlockMap {
    pub fn new() -> Self {
        ObjectBlockMap {
            map: BTreeSet::new(),
        }
    }

    pub fn verify(&self) {
        let mut prev_ent_opt: Option<ObjectBlockMapEntry> = None;
        for ent in self.map.iter() {
            if let Some(prev_ent) = prev_ent_opt {
                assert_gt!(ent.object, prev_ent.object);
                assert_gt!(ent.block, prev_ent.block);
            }
            prev_ent_opt = Some(*ent);
        }
    }

    pub fn insert(&mut self, object: ObjectID, block: BlockID) {
        // verify that this block is between the existing entries blocks
        let prev_ent_opt = self.map.range((Unbounded, Excluded(object))).next_back();
        if let Some(prev_ent) = prev_ent_opt {
            assert_lt!(prev_ent.block, block);
        }
        let next_ent_opt = self.map.range((Excluded(object), Unbounded)).next();
        if let Some(next_ent) = next_ent_opt {
            assert_gt!(next_ent.block, block);
        }
        // verify that this object is not yet in the map
        assert!(!self.map.contains(&object));

        self.map.insert(ObjectBlockMapEntry { object, block });
    }

    pub fn remove(&mut self, object: ObjectID) {
        let removed = self.map.remove(&object);
        assert!(removed);
    }

    pub fn block_to_object(&self, block: BlockID) -> ObjectID {
        self.map
            .range((Unbounded, Included(block)))
            .next_back()
            .unwrap()
            .object
    }

    pub fn object_to_min_block(&self, object: ObjectID) -> BlockID {
        self.map.get(&object).unwrap().block
    }

    pub fn object_to_next_block(&self, object: ObjectID) -> BlockID {
        self.map
            .range((Excluded(object), Unbounded))
            .next()
            .unwrap()
            .block
    }

    pub fn last_object(&self) -> ObjectID {
        self.map
            .iter()
            .next_back()
            .unwrap_or(&ObjectBlockMapEntry {
                object: ObjectID(0),
                block: BlockID(0),
            })
            .object
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn iter(&self) -> std::collections::btree_set::Iter<ObjectBlockMapEntry> {
        self.map.iter()
    }
}
