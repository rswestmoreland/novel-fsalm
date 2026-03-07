// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use fsa_lm::artifact::FsArtifactStore;
use fsa_lm::wiki_ingest::{ingest_wiki_tsv, ingest_wiki_xml, WikiIngestCfg};

#[test]
fn wiki_ingest_xml_matches_tsv_for_simple_input() {
    let dir = std::env::temp_dir().join("fsa_lm_tests").join("wiki_ingest_xml_matches_tsv");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let store = FsArtifactStore::new(&dir).unwrap();

    let tsv = "Hello\thi there\n";
    let xml = "<mediawiki><page><title>Hello</title><ns>0</ns><revision><text xml:space=\"preserve\">hi there</text></revision></page></mediawiki>";

    let mut cfg = WikiIngestCfg::default_v1();
    cfg.seg_rows = 8;
    cfg.chunk_rows = 16;
    cfg.row_max_bytes = 1024;
    cfg.max_docs = Some(10);

    let h_tsv = ingest_wiki_tsv(&store, std::io::BufReader::new(std::io::Cursor::new(tsv.as_bytes())), cfg).unwrap();
    let h_xml = ingest_wiki_xml(&store, std::io::BufReader::new(std::io::Cursor::new(xml.as_bytes())), cfg).unwrap();

    assert_eq!(h_tsv, h_xml);
}
