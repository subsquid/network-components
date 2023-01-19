use std::cmp::{max, Ordering};
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


#[derive(Clone, Serialize, Deserialize)]
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
            let p = ranges[pi];
            if c.begin() > p.end() + 1 {
                pi += 1;
                ranges[pi] = c;
            } else {
                ranges[pi] = Range(
                    p.begin(),
                    max(c.end(), p.end())
                );
            }
        }
        ranges.truncate(pi + 1);
        RangeSet(ranges.into_boxed_slice())
    }
}


impl std::fmt::Debug for RangeSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RangeSet{:?}", self.0)
    }
}


#[cfg(test)]
mod tests {
    use crate::range::{Range, RangeSet};

    #[test]
    fn range_set_always_sorted_and_separated() {
        let rs = RangeSet::from(vec![
            Range(10, 20),
            Range(21, 30),
            Range(35, 40),
            Range(5, 10)
        ]);
        assert_eq!(rs.0, vec![
            Range(5, 30),
            Range(35, 40)
        ].into());
    }

    #[test]
    fn range_set_has() {
        let rs = RangeSet::from(vec![
            Range(0, 10),
            Range(20, 30),
            Range(40, 50)
        ]);

        assert!(rs.has(0));
        assert!(rs.has(5));
        assert!(rs.has(10));
        assert!(rs.has(21));
        assert!(rs.has(40));
        assert!(rs.has(41));
        assert!(rs.has(50));
        assert!(!rs.has(11));
        assert!(!rs.has(19));
        assert!(!rs.has(35));
    }

    #[test]
    fn range_set_find_containing_range() {
        let rs = RangeSet::from(vec![
            Range(0, 10),
            Range(20, 30),
            Range(40, 50)
        ]);

        assert_eq!(rs.find_containing_range(0), Some(Range(0, 10)));
        assert_eq!(rs.find_containing_range(5), Some(Range(0, 10)));
        assert_eq!(rs.find_containing_range(30), Some(Range(20, 30)));
        assert_eq!(rs.find_containing_range(41), Some(Range(40, 50)));
        assert_eq!(rs.find_containing_range(15), None);
    }
}
