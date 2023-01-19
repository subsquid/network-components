use std::cmp::Ordering;
use serde::{Serialize, Deserialize};


#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize, Debug)]
pub struct Range(u32, u32);


impl Range {
    pub fn new(beg: u32, end: u32) -> Self {
        assert!(beg <= end);
        Range(beg, end)
    }

    #[inline]
    pub fn begin(&self) -> u32 {
        self.0
    }

    #[inline]
    pub fn end(&self) -> u32 {
        self.1
    }
}


#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct RangeSet(Box<[Range]>);


impl RangeSet {
    pub fn empty() -> Self {
        RangeSet(Box::new([]))
    }

    pub fn has(&self, point: u32) -> bool {
        self.containing_range(point).is_some()
    }

    pub fn find_containing_range(&self, point: u32) -> Option<Range> {
        self.containing_range(point).map(|i| self.0[i])
    }

    fn containing_range(&self, point: u32) -> Option<usize> {
        self.0.binary_search_by(|i| {
            if point < i.begin() {
                Ordering::Greater
            } else if i.end() < point {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        }).ok()
    }

    pub fn includes(&self, range: Range) -> bool {
        if let Some(c) = self.find_containing_range(range.begin()) {
            c.end() >= range.end()
        } else {
            false
        }
    }
}


impl From<Vec<Range>> for RangeSet {
    fn from(mut ranges: Vec<Range>) -> Self {
        if ranges.len() == 0 {
            return RangeSet::empty()
        }
        ranges.sort();
        let mut pi = 0;
        for i in 1..ranges.len() {
            let c = ranges[i];
            let mut p = ranges[pi];
            if c.begin() > p.end() {
                pi += 1;
                ranges[pi] = c;
            } else {
                p.1 = c.end();
            }
        }
        ranges.truncate(pi + 1);
        RangeSet(ranges.into_boxed_slice())
    }
}
