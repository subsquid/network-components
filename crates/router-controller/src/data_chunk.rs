use crate::range::Range;

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct DataChunk {
    top: u32,
    first_block: u32,
    last_block: u32
}


impl DataChunk {
    pub fn new(top: u32, first_block: u32, last_block: u32) -> Self {
        assert!(top <= first_block);
        assert!(first_block <= last_block);
        DataChunk {
            top,
            first_block,
            last_block
        }
    }

    #[inline]
    pub fn top(&self) -> u32 {
        self.top
    }

    #[inline]
    pub fn first_block(&self) -> u32 {
        self.first_block
    }

    #[inline]
    pub fn last_block(&self) -> u32 {
        self.last_block
    }
}


impl std::fmt::Display for DataChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:010}/{:010}-{:010}", self.top, self.first_block, self.last_block)
    }
}


impl std::fmt::Debug for DataChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}


impl std::str::FromStr for DataChunk {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let top_range_split = s.find('/').ok_or(())?;
        let (top_str, range_str) = s.split_at(top_range_split);
        let top: u32 = top_str.parse().or(Err(()))?;
        let beg_end_split = range_str.find('-').ok_or(())?;
        let (beg_str, end_str) = range_str.split_at(beg_end_split);
        let beg: u32 = beg_str.parse().or(Err(()))?;
        let end: u32 = end_str.parse().or(Err(()))?;
        if top <= beg && beg <= end {
            Ok(DataChunk::new(top, beg, end))
        } else {
            Err(())
        }
    }
}


impl From<DataChunk> for Range {
    fn from(chunk: DataChunk) -> Self {
        Range::new(chunk.first_block, chunk.last_block)
    }
}
