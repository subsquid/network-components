use std::cmp::Ordering;


#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct Range {
    beg: u32,
    end: u32
}


impl Range {
    pub fn new(beg: u32, end: u32) -> Self {
        assert!(beg <= end);
        Range { beg, end }
    }

    #[inline]
    pub fn begin(&self) -> u32 {
        self.beg
    }

    #[inline]
    pub fn end(&self) -> u32 {
        self.end
    }
}


#[derive(Clone)]
pub struct RangeSet {
    ranges: Box<[Range]>
}


impl RangeSet {
    pub fn empty() -> Self {
        RangeSet {
            ranges: Box::new([])
        }
    }

    pub fn has(&self, point: u32) -> bool {
        self.containing_range(point).is_some()
    }

    pub fn find_containing_range(&self, point: u32) -> Option<Range> {
        self.containing_range(point).map(|i| self.ranges[i])
    }

    fn containing_range(&self, point: u32) -> Option<usize> {
        self.ranges.binary_search_by(|i| {
            if point < i.beg {
                Ordering::Greater
            } else if i.end < point {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        }).ok()
    }

    pub fn includes(&self, range: Range) -> bool {
        if let Some(c) = self.find_containing_range(range.beg) {
            c.end >= range.end
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
            if c.beg > p.end {
                pi += 1;
                ranges[pi] = c;
            } else {
                p.end = c.end;
            }
        }
        ranges.truncate(pi + 1);
        RangeSet {
            ranges: ranges.into_boxed_slice()
        }
    }
}
