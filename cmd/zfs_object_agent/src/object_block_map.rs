use crate::base_types::*;
use more_asserts::*;
use std::borrow::Borrow;
use std::collections::BTreeSet;
use std::ops::Bound::*;
use std::sync::RwLock;

#[derive(Debug, Default)]
pub struct ObjectBlockMap {
    map: RwLock<BTreeSet<ObjectBlockMapEntry>>,
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
        ObjectBlockMap::default()
    }

    pub fn verify(&self) {
        let mut prev_ent_opt: Option<ObjectBlockMapEntry> = None;
        for ent in self.map.read().unwrap().iter() {
            if let Some(prev_ent) = prev_ent_opt {
                assert_gt!(ent.object, prev_ent.object);
                assert_gt!(ent.block, prev_ent.block);
            }
            prev_ent_opt = Some(*ent);
        }
    }

    pub fn insert(&self, object: ObjectID, block: BlockID) {
        // verify that this block is between the existing entries blocks
        let mut map = self.map.write().unwrap();
        let prev_ent_opt = map.range((Unbounded, Excluded(object))).next_back();
        if let Some(prev_ent) = prev_ent_opt {
            assert_lt!(prev_ent.block, block);
        }
        let next_ent_opt = map.range((Excluded(object), Unbounded)).next();
        if let Some(next_ent) = next_ent_opt {
            assert_gt!(next_ent.block, block);
        }
        // verify that this object is not yet in the map
        assert!(!map.contains(&object));

        map.insert(ObjectBlockMapEntry { object, block });
    }

    pub fn remove(&self, object: ObjectID) {
        let removed = self.map.write().unwrap().remove(&object);
        assert!(removed);
    }

    pub fn block_to_object(&self, block: BlockID) -> ObjectID {
        self.map
            .read()
            .unwrap()
            .range((Unbounded, Included(block)))
            .next_back()
            .unwrap()
            .object
    }

    pub fn object_to_min_block(&self, object: ObjectID) -> BlockID {
        self.map.read().unwrap().get(&object).unwrap().block
    }

    pub fn object_to_next_block(&self, object: ObjectID) -> BlockID {
        self.map
            .read()
            .unwrap()
            .range((Excluded(object), Unbounded))
            .next()
            .unwrap()
            .block
    }

    pub fn last_object(&self) -> ObjectID {
        self.map
            .read()
            .unwrap()
            .iter()
            .next_back()
            .unwrap_or(&ObjectBlockMapEntry {
                object: ObjectID(0),
                block: BlockID(0),
            })
            .object
    }

    pub fn len(&self) -> usize {
        self.map.read().unwrap().len()
    }

    pub fn for_each<CB>(&self, mut f: CB)
    where
        CB: FnMut(&ObjectBlockMapEntry),
    {
        for ent in self.map.read().unwrap().iter() {
            f(ent);
        }
    }
}
