#[derive(Clone)]
pub struct DataRange {
    pub from: i32,
    pub to: i32,
}

/// Directory in the dataset containing data for a certain range of blocks.
pub struct DataDir {
    /// First block covered by the dir.
    from: i32,
    /// Last block covered by the dir (inclusive).
    to: i32,
    /// Size of dir in bytes.
    size: i64,
}

pub fn get_dataset_range() -> DataRange {
    let ranges = get_data_ranges();

    if ranges.is_empty() {
        return DataRange { from: -1, to: -1 };
    }

    DataRange {
        from: ranges.first().unwrap().from,
        to: ranges.last().unwrap().to,
    }
}

/// Get data directories in the dataset.
fn get_data_directories() -> Vec<DataDir> {
    vec![]
}

/**
Get dataset ranges (data scheduling units).

The resulting ranges are sorted and guaranteed to cover continues range of blocks
starting from block 0.
*/
pub fn get_data_ranges() -> Vec<DataRange> {
    let mut dirs = get_data_directories();
    dirs.sort_by_key(|dir| dir.from);
    let mut ranges = vec![];

    for dir in dirs {
        if ranges.is_empty() {
            if dir.from == 0 {
                ranges.push(dir);
            } else {
                break;
            }
        } else {
            let current = ranges.last_mut().unwrap();
            if current.to + 1 != dir.from {
                break;
            }
            if current.size > 20 * 1024 * 1024 * 1024 {
                ranges.push(dir);
            } else {
                current.size = dir.size;
                current.to = dir.to;
            }
        }
    }

    // TODO: avoid extra iteration
    ranges
        .into_iter()
        .map(|dir| DataRange {
            from: dir.from,
            to: dir.to,
        })
        .collect()
}
