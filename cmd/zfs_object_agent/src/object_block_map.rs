use crate::pool::*;
use std::borrow::Borrow;
use std::collections::BTreeSet;
use std::ops::Bound::*;

#[derive(Debug)]
pub struct ObjectBlockMap {
    map: BTreeSet<ObjectBlockMapEntry>,
}

#[derive(Debug, Ord, PartialOrd, PartialEq, Eq, Copy, Clone)]
struct ObjectBlockMapEntry {
    obj: OBMEObject,
    block: OBMEBlock,
}

#[derive(Debug, Ord, PartialOrd, PartialEq, Eq, Copy, Clone)]
struct OBMEObject(ObjectID);

impl Borrow<OBMEObject> for ObjectBlockMapEntry {
    fn borrow(&self) -> &OBMEObject {
        &self.obj
    }
}
#[derive(Debug, Ord, PartialOrd, PartialEq, Eq, Copy, Clone)]
struct OBMEBlock(BlockID);

impl Borrow<OBMEBlock> for ObjectBlockMapEntry {
    fn borrow(&self) -> &OBMEBlock {
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
                assert!(ent.obj > prev_ent.obj);
                assert!(ent.block > prev_ent.block);
            }
            prev_ent_opt = Some(*ent);
        }
    }

    pub fn insert(&mut self, obj: ObjectID, block: BlockID) {
        // verify that this block is between the existing entries blocks
        let prev_ent_opt = self
            .map
            .range((Unbounded, Excluded(OBMEObject(obj))))
            .next_back();
        if let Some(prev_ent) = prev_ent_opt {
            assert!(prev_ent.block.0 < block);
        }
        let next_ent_opt = self
            .map
            .range((Excluded(OBMEObject(obj)), Unbounded))
            .next();
        if let Some(next_ent) = next_ent_opt {
            assert!(next_ent.block.0 > block);
        }
        // verify that this object is not yet in the map
        assert!(!self.map.contains(&OBMEObject(obj)));

        self.map.insert(ObjectBlockMapEntry {
            obj: OBMEObject(obj),
            block: OBMEBlock(block),
        });
    }

    pub fn remove(&mut self, obj: ObjectID) {
        let removed = self.map.remove(&OBMEObject(obj));
        assert!(removed);
    }

    pub fn block_to_obj(&self, block: BlockID) -> ObjectID {
        self.map
            .range((Unbounded, Included(OBMEBlock(block))))
            .next_back()
            .unwrap()
            .obj
            .0
    }

    pub fn obj_to_block(&self, obj: ObjectID) -> BlockID {
        self.map.get(&OBMEObject(obj)).unwrap().block.0
    }

    pub fn last_obj(&self) -> ObjectID {
        self.map
            .iter()
            .next_back()
            .unwrap_or(&ObjectBlockMapEntry {
                obj: OBMEObject(ObjectID(0)),
                block: OBMEBlock(BlockID(0)),
            })
            .obj
            .0
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }
}
