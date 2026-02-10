use smallvec::SmallVec;

use super::MergableSpan;

pub trait AppendRle<T: MergableSpan> {
    /// Push a new item to this list-like object. If the passed item can be merged into the
    /// last item in the list, do so instead of inserting a new item.
    ///
    /// Returns true if the item was merged into the previous last item, false if it was inserted.
    fn push_rle(&mut self, item: T) -> bool;

    /// Push a new item to the end of this list-like object. If the passed object can be merged
    /// to the front of the previously last item, do so. This is useful for appending to a list
    /// which is sorted in reverse.
    fn push_reversed_rle(&mut self, item: T) -> bool;

    /// Extend the item by RLE-compacting the incoming iterator.
    fn extend_rle<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        for item in iter {
            self.push_rle(item);
        }
    }
}

impl<T: MergableSpan> AppendRle<T> for Vec<T> {
    fn push_rle(&mut self, item: T) -> bool {
        if let Some(v) = self.last_mut()
            && v.can_append(&item)
        {
            v.append(item);
            return true;
        }

        self.push(item);
        false
    }

    fn push_reversed_rle(&mut self, item: T) -> bool {
        if let Some(v) = self.last_mut()
            && item.can_append(v)
        {
            v.prepend(item);
            return true;
        }

        self.push(item);
        false
    }
}

impl<T, const N: usize> AppendRle<T> for SmallVec<T, N> where T: MergableSpan {
    fn push_rle(&mut self, item: T) -> bool {
        if let Some(v) = self.last_mut()
            && v.can_append(&item)
        {
            v.append(item);
            return true;
        }

        self.push(item);
        false
    }

    fn push_reversed_rle(&mut self, item: T) -> bool {
        if let Some(v) = self.last_mut()
            && item.can_append(v)
        {
            v.prepend(item);
            return true;
        }

        self.push(item);
        false
    }
}
