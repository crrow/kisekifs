use rangemap::RangeMap;
use std::cmp::min;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

type ValidRange = usize;
#[derive(Clone, Debug)]
struct SliceReader {
    valid: Arc<AtomicBool>,
    range: Range<usize>,
}

impl PartialEq for SliceReader {
    fn eq(&self, other: &Self) -> bool {
        self.range == other.range
    }
}

impl Eq for SliceReader {}

fn new_slice_reader(r: Range<usize>, valid: bool) -> Arc<SliceReader> {
    Arc::new(SliceReader {
        valid: Arc::new(AtomicBool::new(valid)),
        range: r,
    })
}

struct Req {
    sub_range: Range<usize>,
    sr: Arc<SliceReader>,
}

/**
Question, do the following case exists? we need to make a SliceReader for just one bit.

insert slice-reader: 0..512
insert slice-reader: 520..720
insert slice-reader: 720..1021
insert slice-reader: 1021..1022
insert slice-reader: 1022..1023
invalidate 720 ~ 1021.
invalidate 1021 ~ 1022.

expect read: 1020..1024

find gap 1023..1024, need to make req
want: 1020..1024, overlapping but invalid range 720..1021, cut result: 1020..1021
want: 1020..1024, overlapping but invalid range 1021..1022, cut result: 1021..1022
after merge: want: 1020..1024, result: 1020..1022
after merge: want: 1020..1024, result: 1023..1024
*/
fn make_req_from_range(rmap: &RangeMap<ValidRange, Arc<SliceReader>>, cur_range: Range<usize>) {
    // let mut reqs = Vec::new();
    let mut rs = rangemap::RangeSet::new();
    rmap.gaps(&cur_range).for_each(|gap| {
        println!("find gap {:?}, need to make req", gap);
        rs.insert(gap);
    });
    rmap.overlapping(&cur_range).for_each(|(vr, sr)| {
        if !sr.valid.load(Ordering::Acquire) {
            // cut the unnecessary part
            let real = if cur_range.start < vr.start {
                // current:     [----]
                // slice-reader:  [----]
                vr.start..min(cur_range.end, vr.end) // only get the overlapping part.
            } else {
                // current:         [----]
                // slice-reader:  [----]
                cur_range.start..min(cur_range.end, vr.end)
            };
            rs.insert(real.clone());
            println!(
                "want: {:?}, overlapping but invalid range {:?}, cut result: {:?}",
                cur_range.clone(),
                vr,
                real.clone(),
            );
        }
    });

    rs.iter()
        .for_each(|r| println!("after merge: want: {:?}, result: {:?}", cur_range, r));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic() {
        let mut map = RangeMap::new();
        let r = 0..512;
        map.insert(r.clone(), new_slice_reader(r.clone(), false));
        println!("insert slice-reader: {:?}", r);
        // map.insert(512..520, new_slice_reader(512..520));
        let r = 520..720;
        map.insert(r.clone(), new_slice_reader(r.clone(), false));
        println!("insert slice-reader: {:?}", r);
        let r = 720..1021;
        map.insert(r.clone(), new_slice_reader(r.clone(), false));
        println!("insert slice-reader: {:?}", r);
        let r = 1021..1022;
        map.insert(r.clone(), new_slice_reader(r.clone(), false));
        println!("insert slice-reader: {:?}", r);
        let r = 1022..1023;
        map.insert(r.clone(), new_slice_reader(r.clone(), true));
        println!("insert slice-reader: {:?}", r);

        make_req_from_range(&map, 1020..1024);
    }
}
