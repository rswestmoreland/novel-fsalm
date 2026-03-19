// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use fsa_lm::artifact::FsArtifactStore;
use fsa_lm::context_anchors::{CA_FLAG_USED_LEXICON, CONTEXT_ANCHORS_V1_VERSION};
use fsa_lm::context_anchors_artifact::get_context_anchors_v1;
use fsa_lm::conversation_pack::ConversationRole;
use fsa_lm::conversation_pack_artifact::get_conversation_pack;
use fsa_lm::hash::Hash32;
use fsa_lm::replay_artifact::get_replay_log;
use fsa_lm::replay_steps::STEP_CONTEXT_ANCHORS_V1;
use fsa_lm::tokenizer::{term_id_from_token, TokenizerCfg};

use std::path::{Path, PathBuf};
use std::process::Command;

fn tmp_dir(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push("fsa_lm_tests");
    p.push(format!("{}_{}", name, std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn run_cmd(bin: &str, args: &[&str]) -> (i32, String, String) {
    let out = Command::new(bin).args(args).output().unwrap();
    let code = out.status.code().unwrap_or(-1);
    let stdout = String::from_utf8_lossy(&out.stdout).replace("\r\n", "\n");
    let stderr = String::from_utf8_lossy(&out.stderr).replace("\r\n", "\n");
    (code, stdout, stderr)
}

fn write_wiki_tsv(path: &Path) {
    let line = "banana\tbanana banana is a fruit\n";
    std::fs::write(path, line.as_bytes()).unwrap();
}

fn write_wiktionary_xml(path: &Path) {
    let xml = r#"<mediawiki>
<page>
  <title>banana</title>
  <ns>0</ns>
  <revision>
    <text xml:space=\"preserve\">==English==
===Noun===
# A banana is a fruit.
====Synonyms====
* [[plantain|plantain]]
</text>
  </revision>
</page>
</mediawiki>"#;
    std::fs::write(path, xml.as_bytes()).unwrap();
}

fn is_hex64(s: &str) -> bool {
    s.len() == 64 && s.as_bytes().iter().all(|b| b"0123456789abcdef".contains(b))
}

fn parse_first_hex(stdout: &str) -> Option<String> {
    for line in stdout.lines() {
        let v = line.trim();
        if is_hex64(v) {
            return Some(v.to_string());
        }
    }
    None
}

fn parse_stderr_kv(stderr: &str, key: &str) -> Option<String> {
    let prefix = format!("{}=", key);
    for line in stderr.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix(&prefix) {
            let v = rest.trim();
            if is_hex64(v) {
                return Some(v.to_string());
            }
        }
    }
    None
}

fn parse_hash_line(stdout: &str, key: &str) -> Option<String> {
    let prefix = format!("{}=", key);
    for line in stdout.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix(&prefix) {
            let v = rest.trim();
            if is_hex64(v) {
                return Some(v.to_string());
            }
        }
    }
    None
}

fn parse_file_kv(path: &Path, key: &str) -> Option<String> {
    let s = std::fs::read_to_string(path).ok()?.replace("\r\n", "\n");
    let prefix = format!("{}=", key);
    let mut last: Option<String> = None;
    for raw in s.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix(&prefix) {
            let v = rest.trim();
            if is_hex64(v) {
                last = Some(v.to_string());
            }
        }
    }
    last
}

fn hex_to_hash32(s: &str) -> Hash32 {
    fsa_lm::hash::parse_hash32_hex(s).unwrap()
}

fn write_workspace(root: &Path, merged_snapshot: &str, merged_sig_map: &str, lexicon_snapshot: &str) {
    let mut s = String::new();
    s.push_str(&format!("merged_snapshot={}\n", merged_snapshot));
    s.push_str(&format!("merged_sig_map={}\n", merged_sig_map));
    s.push_str(&format!("lexicon_snapshot={}\n", lexicon_snapshot));
    std::fs::write(root.join("workspace_v1.txt"), s.as_bytes()).unwrap();
}

#[test]
fn context_anchors_are_recorded_in_ask_sessions() {
    let base = tmp_dir("context_anchors_are_recorded_in_ask_sessions");
    let root = base.join("root");
    std::fs::create_dir_all(&root).unwrap();

    let bin = env!("CARGO_BIN_EXE_fsa_lm");

    // Build minimal wiki index.
    let wiki_tsv = base.join("wiki.tsv");
    write_wiki_tsv(&wiki_tsv);

    let (wcode, _wout, werr) = run_cmd(
        bin,
        &[
            "ingest-wiki",
            "--dump",
            wiki_tsv.to_str().unwrap(),
            "--root",
            root.to_str().unwrap(),
            "--max_docs",
            "1",
        ],
    );
    assert_eq!(wcode, 0, "stderr={}", werr);

    let (bcode, bout, berr) = run_cmd(bin, &["build-index", "--root", root.to_str().unwrap()]);
    assert_eq!(bcode, 0, "stderr={}", berr);
    let idx_snap_hex = parse_first_hex(&bout).expect("snapshot hash on stdout");
    let sig_map_hex = parse_stderr_kv(&berr, "index_sig_map").expect("index_sig_map on stderr");

    // Build minimal lexicon snapshot.
    let xml_path = base.join("wiktionary.xml");
    write_wiktionary_xml(&xml_path);

    let (lxcode, lxout, lxerr) = run_cmd(
        bin,
        &[
            "ingest-wiktionary-xml",
            "--root",
            root.to_str().unwrap(),
            "--xml",
            xml_path.to_str().unwrap(),
            "--segments",
            "2",
            "--max_pages",
            "10",
        ],
    );
    assert_eq!(lxcode, 0, "stderr={}", lxerr);
    let lex_snap_hex = parse_hash_line(&lxout, "lexicon_snapshot").expect("lexicon_snapshot line");

    write_workspace(&root, &idx_snap_hex, &sig_map_hex, &lex_snap_hex);

    let session_file = base.join("session.txt");

    // Turn 1: establish a prior turn mentioning banana.
    let (a1, _out1, err1) = run_cmd(
        bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_file.to_str().unwrap(),
            "--expand",
            "--text",
            "Tell me about banana",
        ],
    );
    assert_eq!(a1, 0, "stderr={}", err1);

    // Turn 2: follow-up question should record context anchors.
    let (a2, _out2, err2) = run_cmd(
        bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_file.to_str().unwrap(),
            "--expand",
            "--text",
            "Why is it a fruit?",
        ],
    );
    assert_eq!(a2, 0, "stderr={}", err2);

    let conv_hex = parse_file_kv(&session_file, "conversation_pack").expect("conversation_pack in session file");
    let conv_hash = hex_to_hash32(&conv_hex);

    let store = FsArtifactStore::new(&root).unwrap();
    let pack = get_conversation_pack(&store, &conv_hash).unwrap().unwrap();

    let mut replays: Vec<Hash32> = Vec::new();
    for m in &pack.messages {
        if m.role == ConversationRole::Assistant {
            if let Some(r) = m.replay_id {
                replays.push(r);
            }
        }
    }
    assert!(replays.len() >= 2, "expected at least two assistant turns");

    let replay2 = replays[1];
    let rlog = get_replay_log(&store, &replay2).unwrap().unwrap();

    let mut anchors_hash_opt: Option<Hash32> = None;
    for st in &rlog.steps {
        if st.name == STEP_CONTEXT_ANCHORS_V1 {
            if !st.outputs.is_empty() {
                anchors_hash_opt = Some(st.outputs[0]);
            }
        }
    }
    let anchors_hash = anchors_hash_opt.expect("context-anchors-v1 step");

    let ca = get_context_anchors_v1(&store, &anchors_hash).unwrap().unwrap();
    assert_eq!(ca.version, CONTEXT_ANCHORS_V1_VERSION);
    assert!((ca.flags & CA_FLAG_USED_LEXICON) != 0, "expected lexicon usage flag");

    let tok_cfg = TokenizerCfg { max_token_bytes: 32 };
    let banana_tid = term_id_from_token("banana", tok_cfg);
    let banana_u64 = (banana_tid.0).0;

    let mut found = false;
    for t in &ca.terms {
        if t.term_id.0 == banana_u64 {
            found = true;
            break;
        }
    }
    assert!(found, "expected banana term id in context anchors");
}
