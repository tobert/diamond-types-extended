//! Run-length encoding utilities, vendored from the `rle` crate (v0.2.0)
//! by Joseph Gentle. License: ISC OR Apache-2.0
//! Original: <https://github.com/josephg/diamond-types/tree/master/crates/rle>

use std::fmt::Debug;
use std::ops::Range;

/// Checks if the entry contains the specified item. If it does, returns the offset into the item.
pub trait Searchable {
    type Item: Copy + Debug;

    fn get_offset(&self, loc: Self::Item) -> Option<usize>;
    fn at_offset(&self, offset: usize) -> Self::Item;
}

pub trait HasRleKey {
    fn rle_key(&self) -> usize;
}

impl<T> HasRleKey for &T where T: HasRleKey {
    fn rle_key(&self) -> usize {
        (*self).rle_key()
    }
}

impl HasRleKey for Range<usize> {
    fn rle_key(&self) -> usize {
        self.start
    }
}

impl HasRleKey for Range<u32> {
    fn rle_key(&self) -> usize {
        self.start as _
    }
}
