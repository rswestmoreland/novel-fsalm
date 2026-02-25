// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use std::io::{BufReader, Cursor, Write};

use bzip2::read::BzDecoder;
use bzip2::write::BzEncoder;
use bzip2::Compression;

use fsa_lm::artifact::FsArtifactStore;
use fsa_lm::wiki_ingest::{ingest_wiki_xml, WikiIngestCfg};

#[test]
fn wiki_ingest_xml_bz2_matches_plain_xml_for_simple_input() {
    let dir = std::env::temp_dir()
        .join("fsa_lm_tests")
        .join("wiki_ingest_xml_bz2_matches_plain");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let store = FsArtifactStore::new(&dir).unwrap();

    let xml = "<mediawiki><page><title>Hello</title><ns>0</ns><revision><text xml:space=\"preserve\">hi there</text></revision></page></mediawiki>";

    let mut cfg = WikiIngestCfg::default_v1();
    cfg.seg_rows = 8;
    cfg.chunk_rows = 16;
    cfg.row_max_bytes = 1024;
    cfg.max_docs = Some(10);

    let h_xml = ingest_wiki_xml(&store, BufReader::new(Cursor::new(xml.as_bytes())), cfg).unwrap();

    let mut enc = BzEncoder::new(Vec::new(), Compression::best());
    enc.write_all(xml.as_bytes()).unwrap();
    let bz = enc.finish().unwrap();

    let dec = BzDecoder::new(Cursor::new(bz));
    let h_bz2 = ingest_wiki_xml(&store, BufReader::new(dec), cfg).unwrap();

    assert_eq!(h_xml, h_bz2);
}
