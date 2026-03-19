// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use fsa_lm::artifact::FsArtifactStore;
use fsa_lm::context_anchors::ContextAnchorsV1;
use fsa_lm::context_anchors_artifact::get_context_anchors_v1;
use fsa_lm::conversation_pack::ConversationRole;
use fsa_lm::conversation_pack_artifact::get_conversation_pack;
use fsa_lm::hash::{parse_hash32_hex, Hash32};
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

fn write_workspace(root: &Path, merged_snapshot: &str, merged_sig_map: &str, lexicon_snapshot: Option<&str>) {
    let mut s = String::new();
    s.push_str(&format!("merged_snapshot={}\n", merged_snapshot));
    s.push_str(&format!("merged_sig_map={}\n", merged_sig_map));
    if let Some(h) = lexicon_snapshot {
        s.push_str(&format!("lexicon_snapshot={}\n", h));
    }
    std::fs::write(root.join("workspace_v1.txt"), s.as_bytes()).unwrap();
}

fn run_cmd(bin: &str, args: &[&str]) -> (i32, String, String) {
    let out = Command::new(bin).args(args).output().unwrap();
    let code = out.status.code().unwrap_or(-1);
    let stdout = String::from_utf8_lossy(&out.stdout).replace("\r\n", "\n");
    let stderr = String::from_utf8_lossy(&out.stderr).replace("\r\n", "\n");
    (code, stdout, stderr)
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

fn setup_workspace(base_name: &str, with_lexicon: bool) -> (PathBuf, String) {
    let base = tmp_dir(base_name);
    let root = base.join("root");
    std::fs::create_dir_all(&root).unwrap();

    let bin = env!("CARGO_BIN_EXE_fsa_lm").to_string();

    let mut lexicon_snapshot: Option<String> = None;
    if with_lexicon {
        let xml_path = base.join("wiktionary.xml");
        write_wiktionary_xml(&xml_path);
        let (lxcode, lxout, lxerr) = run_cmd(
            &bin,
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
        lexicon_snapshot = Some(parse_hash_line(&lxout, "lexicon_snapshot").expect("lexicon_snapshot line"));
    }

    let wiki_tsv = base.join("wiki.tsv");
    write_wiki_tsv(&wiki_tsv);

    let (wcode, _wout, werr) = run_cmd(
        &bin,
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

    let (bcode, bout, berr) = run_cmd(&bin, &["build-index", "--root", root.to_str().unwrap()]);
    assert_eq!(bcode, 0, "stderr={}", berr);
    let idx_snap_hex = parse_first_hex(&bout).expect("snapshot hash on stdout");
    let sig_map_hex = parse_stderr_kv(&berr, "index_sig_map").expect("index_sig_map= on stderr");
    write_workspace(&root, &idx_snap_hex, &sig_map_hex, lexicon_snapshot.as_deref());

    (root, bin)
}

fn count_occurrences(s: &str, needle: &str) -> usize {
    s.match_indices(needle).count()
}

fn read_followup_context_anchors(root: &Path, session_file: &Path) -> ContextAnchorsV1 {
    let conv_hex = parse_file_kv(session_file, "conversation_pack").expect("conversation_pack in session file");
    let conv_hash: Hash32 = parse_hash32_hex(&conv_hex).unwrap();
    let store = FsArtifactStore::new(root).unwrap();
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
        if st.name == STEP_CONTEXT_ANCHORS_V1 && !st.outputs.is_empty() {
            anchors_hash_opt = Some(st.outputs[0]);
        }
    }
    let anchors_hash = anchors_hash_opt.expect("context-anchors-v1 step");
    get_context_anchors_v1(&store, &anchors_hash).unwrap().unwrap()
}

fn evidence_lines(s: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for line in s.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("[E") {
            out.push(trimmed.to_string());
        }
    }
    out
}

#[test]
fn repeated_ask_output_is_stable_for_same_workspace_and_input() {
    let (root, bin) = setup_workspace("repeated_ask_output_is_stable_for_same_workspace_and_input", false);

    let out1 = root.parent().unwrap().join("ask1.txt");
    let out2 = root.parent().unwrap().join("ask2.txt");

    let args1 = [
        "ask",
        "--root",
        root.to_str().unwrap(),
        "--k",
        "8",
        "--out-file",
        out1.to_str().unwrap(),
        "banana",
    ];
    let args2 = [
        "ask",
        "--root",
        root.to_str().unwrap(),
        "--k",
        "8",
        "--out-file",
        out2.to_str().unwrap(),
        "banana",
    ];

    let (code1, _stdout1, stderr1) = run_cmd(&bin, &args1);
    assert_eq!(code1, 0, "stderr={}", stderr1);
    let (code2, _stdout2, stderr2) = run_cmd(&bin, &args2);
    assert_eq!(code2, 0, "stderr={}", stderr2);

    let s1 = std::fs::read_to_string(&out1).unwrap().replace("\r\n", "\n");
    let s2 = std::fs::read_to_string(&out2).unwrap().replace("\r\n", "\n");

    assert_eq!(s1, s2, "output changed across repeated runs");
    assert_eq!(evidence_lines(&s1), evidence_lines(&s2), "evidence lines changed across repeated runs");
    assert!(!s1.contains("Answer v1"), "output={}", s1);
    assert!(!s1.contains("query_id="), "output={}", s1);
    assert!(s1.contains("[E0]"), "output={}", s1);
}

#[test]
fn ask_session_followup_with_pronoun_preserves_subject_and_evidence() {
    let (root, bin) = setup_workspace("ask_session_followup_with_pronoun_preserves_subject_and_evidence", true);
    let session_file = root.parent().unwrap().join("session.txt");
    let out1 = root.parent().unwrap().join("turn1.txt");
    let out2 = root.parent().unwrap().join("turn2.txt");

    let (code1, _stdout1, stderr1) = run_cmd(
        &bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_file.to_str().unwrap(),
            "--expand",
            "--out-file",
            out1.to_str().unwrap(),
            "--text",
            "Tell me about banana",
        ],
    );
    assert_eq!(code1, 0, "stderr={}", stderr1);

    let (code2, _stdout2, stderr2) = run_cmd(
        &bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_file.to_str().unwrap(),
            "--expand",
            "--out-file",
            out2.to_str().unwrap(),
            "--text",
            "Why is it a fruit?",
        ],
    );
    assert_eq!(code2, 0, "stderr={}", stderr2);

    let s2 = std::fs::read_to_string(&out2).unwrap().replace("\r\n", "\n");
    assert!(!s2.contains("Answer v1"), "output={}", s2);
    assert!(!s2.contains("query_id="), "output={}", s2);
    assert!(s2.contains("[E0]"), "output={}", s2);
    assert!(session_file.exists(), "expected session file to exist");

    let ca = read_followup_context_anchors(&root, &session_file);
    let tok_cfg = TokenizerCfg { max_token_bytes: 32 };
    let banana_tid = term_id_from_token("banana", tok_cfg);
    let banana_u64 = (banana_tid.0).0;
    let found = ca.terms.iter().any(|t| t.term_id.0 == banana_u64);
    assert!(found, "expected banana term id in follow-up context anchors");
}

#[test]
fn free_text_logic_prompt_emits_one_best_clarifier() {
    let (root, bin) = setup_workspace("free_text_logic_prompt_emits_one_best_clarifier", false);

    let prompt = "Alice, Bob, and Carol each have a different fruit. Alice does not have the banana. Bob has the apple. Who has the banana?";

    let (code, stdout, stderr) = run_cmd(
        &bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--text",
            prompt,
        ],
    );
    assert_eq!(code, 0, "stderr={}", stderr);
    assert_eq!(count_occurrences(&stdout, "Quick question:"), 1, "stdout={}", stdout);
    assert!(stdout.contains("possible values"), "stdout={}", stdout);
    assert!(!stdout.contains("Please provide the puzzle using"), "stdout={}", stdout);
}
