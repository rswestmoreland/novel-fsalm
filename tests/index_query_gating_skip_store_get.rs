// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;
use std::sync::Mutex;

use fsa_lm::artifact::{ArtifactError, ArtifactStore, FsArtifactStore};
use fsa_lm::frame::{DocId, FrameRowV1, Id64, SourceId, TermFreq, TermId};
use fsa_lm::frame_segment::FrameSegmentV1;
use fsa_lm::frame_store::put_frame_segment_v1;
use fsa_lm::hash::Hash32;
use fsa_lm::index_query::{search_snapshot_gated, QueryTerm, SearchCfg};
use fsa_lm::index_segment::IndexSegmentV1;
use fsa_lm::index_sig_map::{IndexSigMapEntryV1, IndexSigMapV1};
use fsa_lm::index_sig_map_store::put_index_sig_map_v1;
use fsa_lm::index_snapshot::{IndexSnapshotEntryV1, IndexSnapshotV1};
use fsa_lm::index_snapshot_store::put_index_snapshot_v1;
use fsa_lm::index_store::put_index_segment_v1;
use fsa_lm::segment_sig::SegmentSigV1;
use fsa_lm::segment_sig_store::put_segment_sig_v1;

struct CountingStore {
    inner: FsArtifactStore,
    gets: Mutex<Vec<(Hash32, u64)>>,
}

impl CountingStore {
    fn new(inner: FsArtifactStore) -> CountingStore {
        CountingStore {
            inner,
            gets: Mutex::new(Vec::new()),
        }
    }

    fn bump_get(&self, hash: Hash32) {
        let mut g = self.gets.lock().unwrap();
        for (h, n) in g.iter_mut() {
            if *h == hash {
                *n += 1;
                return;
            }
        }
        g.push((hash, 1));
    }

    fn get_count(&self, hash: Hash32) -> u64 {
        let g = self.gets.lock().unwrap();
        for (h, n) in g.iter() {
            if *h == hash {
                return *n;
            }
        }
        0
    }
}

impl ArtifactStore for CountingStore {
    fn get(&self, hash: &Hash32) -> Result<Option<Vec<u8>>, ArtifactError> {
        self.bump_get(*hash);
        self.inner.get(hash)
    }

    fn put(&self, bytes: &[u8]) -> Result<Hash32, ArtifactError> {
        self.inner.put(bytes)
    }

    fn path_for(&self, hash: &Hash32) -> PathBuf {
        self.inner.path_for(hash)
    }
}

fn test_dir(name: &str) -> PathBuf {
    std::env::temp_dir().join("fsa_lm_tests").join(name)
}

fn make_row(src: SourceId, doc: u64, terms: &[TermId]) -> FrameRowV1 {
    let mut r = FrameRowV1::new(DocId(Id64(doc)), src);
    for &t in terms {
        r.terms.push(TermFreq { term: t, tf: 1 });
    }
    r.recompute_doc_len();
    r
}

fn find_definite_miss(sig: &SegmentSigV1) -> TermId {
    let base: u64 = 1_000_000_000;
    for i in 0..1_000_000u64 {
        let t = TermId(Id64(base + i));
        if !sig.might_contain_term(t) {
            return t;
        }
    }
    panic!("unable to find a definite-miss term id")
}

#[test]
fn query_index_signature_gating_skips_store_load_for_definite_miss_segment() {
    let dir = test_dir("index_query_gating_skip_store_get");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let store = CountingStore::new(FsArtifactStore::new(&dir).unwrap());

    let src = SourceId(Id64(1));

    // Segment 2: does not contain the query term (we will choose a term that is a definite miss).
    let seg2_terms = [TermId(Id64(10)), TermId(Id64(11)), TermId(Id64(12))];
    let row2 = make_row(src, 2, &seg2_terms);
    let seg2 = FrameSegmentV1::from_rows(&[row2], 128).unwrap();
    let seg2_hash = put_frame_segment_v1(&store, &seg2).unwrap();
    let idx2 = IndexSegmentV1::build_from_segment(seg2_hash, &seg2).unwrap();
    let idx2_hash = put_index_segment_v1(&store, &idx2).unwrap();
    let idx2_term_ids: Vec<TermId> = idx2.terms.iter().map(|e| e.term).collect();
    let sig2 = SegmentSigV1::build(idx2_hash, &idx2_term_ids, 2048, 3).unwrap();
    let sig2_hash = put_segment_sig_v1(&store, &sig2).unwrap();

    let qterm = find_definite_miss(&sig2);

    // Segment 1: contains the query term.
    let seg1_terms = [qterm, TermId(Id64(20))];
    let row1 = make_row(src, 1, &seg1_terms);
    let seg1 = FrameSegmentV1::from_rows(&[row1], 128).unwrap();
    let seg1_hash = put_frame_segment_v1(&store, &seg1).unwrap();
    let idx1 = IndexSegmentV1::build_from_segment(seg1_hash, &seg1).unwrap();
    let idx1_hash = put_index_segment_v1(&store, &idx1).unwrap();
    let idx1_term_ids: Vec<TermId> = idx1.terms.iter().map(|e| e.term).collect();
    let sig1 = SegmentSigV1::build(idx1_hash, &idx1_term_ids, 2048, 3).unwrap();
    let sig1_hash = put_segment_sig_v1(&store, &sig1).unwrap();

    // Sig-map includes both segments.
    let mut sig_map = IndexSigMapV1::new(src);
    sig_map.entries.push(IndexSigMapEntryV1 {
        index_seg: idx1_hash,
        sig: sig1_hash,
    });
    sig_map.entries.push(IndexSigMapEntryV1 {
        index_seg: idx2_hash,
        sig: sig2_hash,
    });
    sig_map.entries.sort_by(|a, b| a.index_seg.cmp(&b.index_seg));
    let sig_map_hash = put_index_sig_map_v1(&store, &sig_map).unwrap();

    // Snapshot includes both segments.
    let mut snap = IndexSnapshotV1::new(src);
    snap.entries.push(IndexSnapshotEntryV1 {
        frame_seg: seg1_hash,
        index_seg: idx1_hash,
        row_count: idx1.row_count,
        term_count: idx1.terms.len() as u32,
        postings_bytes: idx1.postings.len() as u32,
    });
    snap.entries.push(IndexSnapshotEntryV1 {
        frame_seg: seg2_hash,
        index_seg: idx2_hash,
        row_count: idx2.row_count,
        term_count: idx2.terms.len() as u32,
        postings_bytes: idx2.postings.len() as u32,
    });
    snap.canonicalize_in_place();
    let snap_hash = put_index_snapshot_v1(&store, &snap).unwrap();

    let query_terms = [QueryTerm { term: qterm, qtf: 1 }];
    let cfg = SearchCfg {
        k: 8,
        entry_cap: 0,
        dense_row_threshold: 1024,
    };

    let (hits, gate) = search_snapshot_gated(&store, &snap_hash, &sig_map_hash, &query_terms, &cfg).unwrap();

    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].frame_seg, seg1_hash);

    // The skipped segment's index artifact should not be loaded.
    assert_eq!(store.get_count(idx2_hash), 0);
    assert!(store.get_count(idx1_hash) > 0);

    // Gate stats should show 1 decoded and 1 skipped.
    assert_eq!(gate.entries_total, 2);
    assert_eq!(gate.entries_decoded, 1);
    assert_eq!(gate.entries_skipped_sig, 1);
    assert_eq!(gate.entries_missing_sig, 0);
}
