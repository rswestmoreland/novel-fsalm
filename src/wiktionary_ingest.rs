// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Wiktionary wikitext scanning helpers.
//!
//! This module provides a deterministic, conservative parser for extracting
//! English-only lexicon signals from Wiktionary page text.
//!
//! The parser uses an explicit allowlist of headings and templates and applies
//! stable caps to keep per-page work bounded. Unknown patterns are ignored.

use crate::lexicon::{
    POS_ADJ, POS_ADV, POS_CONJUNCTION, POS_DETERMINER, POS_INTERJECTION, POS_NOUN, POS_NUMERAL,
    POS_PARTICLE, POS_PREPOSITION, POS_PRONOUN, POS_PROPER_NOUN, POS_VERB,
};

/// Parser configuration for Wiktionary page scanning.
///
/// This configuration enforces deterministic bounds on per-page work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WiktionaryParseCfg {
    /// Maximum page text bytes to scan. Pages larger than this are skipped.
    pub max_page_text_bytes: usize,
    /// Maximum number of senses retained per lemma.
    pub max_senses_per_lemma: usize,
    /// Maximum number of relation targets retained per relation type.
    pub max_relations_per_type: usize,
    /// Maximum number of pronunciations retained per lemma.
    pub max_pronunciations_per_lemma: usize,
    /// Maximum UTF-8 bytes allowed for a single IPA payload.
    pub max_ipa_bytes: usize,
}

impl WiktionaryParseCfg {
    /// Default configuration for v1 ingestion contracts.
    pub fn default_v1() -> Self {
        WiktionaryParseCfg {
            max_page_text_bytes: 128 * 1024,
            max_senses_per_lemma: 16,
            max_relations_per_type: 32,
            max_pronunciations_per_lemma: 8,
            max_ipa_bytes: 96,
        }
    }
}

/// Extracted English lexicon signals for a single Wiktionary page.
///
/// The ingest pipeline will map these signals into lexicon rows and segment
/// artifacts in a later stage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WiktionaryPageExtract {
    /// Page title (lemma text).
    pub title: String,
    /// Union of recognized part-of-speech bits.
    pub pos_mask: u32,
    /// Gloss lines extracted from definition markers (# ...).
    pub senses: Vec<String>,
    /// Synonym relation targets (lemma text).
    pub synonyms: Vec<String>,
    /// Antonym relation targets (lemma text).
    pub antonyms: Vec<String>,
    /// Related terms relation targets (lemma text).
    pub related_terms: Vec<String>,
    /// Hypernym relation targets (lemma text).
    pub hypernyms: Vec<String>,
    /// Hyponym relation targets (lemma text).
    pub hyponyms: Vec<String>,
    /// Derived terms relation targets (lemma text).
    pub derived_terms: Vec<String>,
    /// Coordinate terms relation targets (lemma text).
    pub coordinate_terms: Vec<String>,
    /// Holonym relation targets (lemma text).
    pub holonyms: Vec<String>,
    /// Meronym relation targets (lemma text).
    pub meronyms: Vec<String>,
    /// IPA payload strings extracted from pronunciation templates.
    pub ipas: Vec<String>,
}

/// Parse a single Wiktionary page wikitext.
///
/// Returns None if the page does not contain an English section or if the page
/// violates deterministic bounds (for example, excessive text size).
pub fn parse_wiktionary_page_text(
    title: &str,
    page_text: &str,
    cfg: WiktionaryParseCfg,
) -> Option<WiktionaryPageExtract> {
    if page_text.as_bytes().len() > cfg.max_page_text_bytes {
        return None;
    }

    let mut out = WiktionaryPageExtract {
        title: title.to_string(),
        pos_mask: 0,
        senses: Vec::new(),
        synonyms: Vec::new(),
        antonyms: Vec::new(),
        related_terms: Vec::new(),
        hypernyms: Vec::new(),
        hyponyms: Vec::new(),
        derived_terms: Vec::new(),
        coordinate_terms: Vec::new(),
        holonyms: Vec::new(),
        meronyms: Vec::new(),
        ipas: Vec::new(),
    };

    let mut in_english: bool = false;
    let mut in_allowed_pos: bool = false;
    let mut mode: Mode = Mode::Other;

    for raw in page_text.lines() {
        let line = raw.trim_end_matches('\r');

        if let Some((lvl, head)) = parse_heading(line) {
            match lvl {
                2 => {
                    if in_english {
                        // End of English section.
                        break;
                    }
                    in_english = head == "English";
                    in_allowed_pos = false;
                    mode = Mode::Other;
                }
                3 => {
                    if !in_english {
                        continue;
                    }
                    let pm = pos_mask_from_heading(head);
                    in_allowed_pos = pm != 0;
                    if pm != 0 {
                        out.pos_mask |= pm;
                        mode = Mode::PosBody;
                    } else {
                        mode = Mode::Other;
                    }
                }
                4 => {
                    if !in_english {
                        continue;
                    }
                    mode = mode_from_subheading(head, in_allowed_pos || out.pos_mask != 0);
                }
                _ => {}
            }
            continue;
        }

        if !in_english {
            continue;
        }

        match mode {
            Mode::PosBody => {
                if in_allowed_pos {
                    if out.senses.len() < cfg.max_senses_per_lemma {
                        if let Some(gloss) = parse_sense_line(line) {
                            out.senses.push(gloss);
                        }
                    }
                }
            }
            Mode::Synonyms => {
                collect_relation_targets(line, &mut out.synonyms, cfg.max_relations_per_type);
            }
            Mode::Antonyms => {
                collect_relation_targets(line, &mut out.antonyms, cfg.max_relations_per_type);
            }
            Mode::RelatedTerms => {
                collect_relation_targets(line, &mut out.related_terms, cfg.max_relations_per_type);
            }
            Mode::Hypernyms => {
                collect_relation_targets(line, &mut out.hypernyms, cfg.max_relations_per_type);
            }
            Mode::Hyponyms => {
                collect_relation_targets(line, &mut out.hyponyms, cfg.max_relations_per_type);
            }
            Mode::DerivedTerms => {
                collect_relation_targets(line, &mut out.derived_terms, cfg.max_relations_per_type);
            }
            Mode::CoordinateTerms => {
                collect_relation_targets(
                    line,
                    &mut out.coordinate_terms,
                    cfg.max_relations_per_type,
                );
            }
            Mode::Holonyms => {
                collect_relation_targets(line, &mut out.holonyms, cfg.max_relations_per_type);
            }
            Mode::Meronyms => {
                collect_relation_targets(line, &mut out.meronyms, cfg.max_relations_per_type);
            }
            Mode::Pronunciation => {
                collect_ipa_templates(
                    line,
                    &mut out.ipas,
                    cfg.max_pronunciations_per_lemma,
                    cfg.max_ipa_bytes,
                );
            }
            Mode::Other => {}
        }
    }

    if !in_english {
        return None;
    }

    // Keep pages that produced any signal; allow pos_mask=0 for diagnostics.
    if out.pos_mask == 0
        && out.senses.is_empty()
        && out.synonyms.is_empty()
        && out.antonyms.is_empty()
        && out.related_terms.is_empty()
        && out.hypernyms.is_empty()
        && out.hyponyms.is_empty()
        && out.derived_terms.is_empty()
        && out.coordinate_terms.is_empty()
        && out.holonyms.is_empty()
        && out.meronyms.is_empty()
        && out.ipas.is_empty()
    {
        return None;
    }

    Some(out)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    PosBody,
    Synonyms,
    Antonyms,
    RelatedTerms,
    Hypernyms,
    Hyponyms,
    DerivedTerms,
    CoordinateTerms,
    Holonyms,
    Meronyms,
    Pronunciation,
    Other,
}

fn mode_from_subheading(head: &str, allow: bool) -> Mode {
    if !allow {
        return Mode::Other;
    }
    match head {
        "Synonyms" => Mode::Synonyms,
        "Antonyms" => Mode::Antonyms,
        "Related terms" => Mode::RelatedTerms,
        "Hypernyms" => Mode::Hypernyms,
        "Hyponyms" => Mode::Hyponyms,
        "Derived terms" => Mode::DerivedTerms,
        "Coordinate terms" => Mode::CoordinateTerms,
        "Holonyms" => Mode::Holonyms,
        "Meronyms" => Mode::Meronyms,
        "Pronunciation" => Mode::Pronunciation,
        _ => Mode::PosBody,
    }
}

/// Parse a wikitext heading line.
/// Returns (level, heading_text) for levels 2..=4.
fn parse_heading(line: &str) -> Option<(u8, &str)> {
    let bytes = line.as_bytes();
    if bytes.len() < 4 {
        return None;
    }
    if bytes[0] != b'=' {
        return None;
    }
    let mut n: usize = 0;
    while n < bytes.len() && bytes[n] == b'=' {
        n += 1;
    }
    if n < 2 || n > 4 {
        return None;
    }
    // Trim ASCII whitespace at end, then require matching '=' run.
    let mut end = bytes.len();
    while end > 0 {
        let b = bytes[end - 1];
        if b == b' ' || b == b'\t' {
            end -= 1;
        } else {
            break;
        }
    }
    if end < n * 2 {
        return None;
    }
    for i in 0..n {
        if bytes[end - 1 - i] != b'=' {
            return None;
        }
    }
    let mid = &line[n..(end - n)];
    let head = mid.trim();
    if head.is_empty() {
        return None;
    }
    Some((n as u8, head))
}

fn pos_mask_from_heading(head: &str) -> u32 {
    match head {
        "Noun" => POS_NOUN,
        "Verb" => POS_VERB,
        "Adjective" => POS_ADJ,
        "Adverb" => POS_ADV,
        "Proper noun" => POS_PROPER_NOUN,
        "Pronoun" => POS_PRONOUN,
        "Determiner" => POS_DETERMINER,
        "Preposition" => POS_PREPOSITION,
        "Conjunction" => POS_CONJUNCTION,
        "Interjection" => POS_INTERJECTION,
        "Numeral" => POS_NUMERAL,
        "Particle" => POS_PARTICLE,
        _ => 0,
    }
}

/// Parse a definition line into a gloss string.
///
/// Returns None if the line is not a sense line or is an example line.

fn strip_leading_lb_templates(mut s: &str) -> &str {
    // Strip one or more leading label templates: {{lb|en|...}}.
    // Conservative behavior: if the template is malformed (missing close), stop stripping.
    loop {
        let t = s.trim_start();
        if !t.starts_with("{{lb|en|") {
            return t;
        }
        let end = match t.find("}}") {
            Some(v) => v,
            None => return t,
        };
        s = &t[(end + 2)..];
    }
}

fn parse_sense_line(line: &str) -> Option<String> {
    let s = line.trim_start();
    if !s.starts_with('#') {
        return None;
    }
    // Ignore example lines like "#:" and "##:".
    let mut i: usize = 0;
    let b = s.as_bytes();
    while i < b.len() && b[i] == b'#' {
        i += 1;
    }
    if i < b.len() && b[i] == b':' {
        return None;
    }
    let gloss0 = s[i..].trim();
    let gloss = strip_leading_lb_templates(gloss0).trim();
    if gloss.is_empty() {
        return None;
    }
    Some(gloss.to_string())
}

fn collect_relation_targets(line: &str, out: &mut Vec<String>, cap: usize) {
    if out.len() >= cap {
        return;
    }
    collect_wikilinks(line, out, cap);
    if out.len() >= cap {
        return;
    }
    collect_l_templates(line, out, cap);
}

fn collect_wikilinks(line: &str, out: &mut Vec<String>, cap: usize) {
    let mut s = line;
    while out.len() < cap {
        let i = match s.find("[[") {
            Some(v) => v,
            None => break,
        };
        let rest = &s[(i + 2)..];
        let j = match rest.find("]]") {
            Some(v) => v,
            None => break,
        };
        let inner = &rest[..j];
        let target = match inner.find('|') {
            Some(k) => inner[..k].trim(),
            None => inner.trim(),
        };
        if !target.is_empty() {
            out.push(target.to_string());
        }
        s = &rest[(j + 2)..];
    }
}

fn template_first3(inner: &str) -> Option<(&str, &str, &str)> {
    // Parse the first three '|' separated fields without allocating.
    // This matches prior behavior that only read parts[0..=2] and ignored any extras.
    let mut it = inner.split('|');
    let a = it.next()?.trim();
    let b = it.next()?.trim();
    let c = it.next()?.trim();
    Some((a, b, c))
}

fn collect_l_templates(line: &str, out: &mut Vec<String>, cap: usize) {
    let mut s = line;

    while out.len() < cap {
        let i = match s.find("{{") {
            Some(v) => v,
            None => break,
        };
        let rest = &s[(i + 2)..];
        let j = match rest.find("}}") {
            Some(v) => v,
            None => break,
        };
        let inner = &rest[..j];
        if let Some((tname, lang, target)) = template_first3(inner) {
            if (tname == "l" || tname == "m") && lang == "en" {
                if !target.is_empty() {
                    out.push(target.to_string());
                }
            }
        }
        s = &rest[(j + 2)..];
    }
}

fn collect_ipa_templates(line: &str, out: &mut Vec<String>, cap: usize, max_ipa_bytes: usize) {
    if out.len() >= cap {
        return;
    }
    let mut s = line;

    while out.len() < cap {
        let i = match s.find("{{") {
            Some(v) => v,
            None => break,
        };
        let rest = &s[(i + 2)..];
        let j = match rest.find("}}") {
            Some(v) => v,
            None => break,
        };
        let inner = &rest[..j];
        if let Some((tname, lang, ipa)) = template_first3(inner) {
            if tname == "IPA" && lang == "en" {
                if !ipa.is_empty() && ipa.as_bytes().len() <= max_ipa_bytes {
                    out.push(ipa.to_string());
                }
            }
        }
        s = &rest[(j + 2)..];
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_skips_pages_without_english() {
        let cfg = WiktionaryParseCfg::default_v1();
        let txt = "==French==\n===Noun===\n# test\n";
        let got = parse_wiktionary_page_text("test", txt, cfg);
        assert!(got.is_none());
    }

    #[test]
    fn parse_basic_english_pos_sense_rel_ipa() {
        let cfg = WiktionaryParseCfg::default_v1();
        let txt = "==English==\n===Noun===\n# A test sense\n#:\n====Synonyms====\n* [[foo]]\n* {{l|en|bar}}\n====Pronunciation====\n* {{IPA|en|/test/}}\n==French==\n===Noun===\n# should not be read\n";
        let got = parse_wiktionary_page_text("Test", txt, cfg).unwrap();
        assert_eq!(got.title, "Test");
        assert_eq!(got.pos_mask, POS_NOUN);
        assert_eq!(got.senses, vec!["A test sense".to_string()]);
        assert_eq!(got.synonyms, vec!["foo".to_string(), "bar".to_string()]);
        assert_eq!(got.ipas, vec!["/test/".to_string()]);
    }

    #[test]
    fn parse_sense_strips_leading_lb_template() {
        let cfg = WiktionaryParseCfg::default_v1();
        let txt = "==English==\n===Noun===\n# {{lb|en|slang}} A test sense\n";
        let got = parse_wiktionary_page_text("x", txt, cfg).unwrap();
        assert_eq!(got.senses, vec!["A test sense".to_string()]);
    }

    #[test]
    fn parse_sense_strips_multiple_leading_lb_templates() {
        let cfg = WiktionaryParseCfg::default_v1();
        let txt = "==English==\n===Noun===\n# {{lb|en|slang}}{{lb|en|foo}}   A test sense\n";
        let got = parse_wiktionary_page_text("x", txt, cfg).unwrap();
        assert_eq!(got.senses, vec!["A test sense".to_string()]);
    }

    #[test]
    fn parse_sense_does_not_strip_non_leading_lb_template() {
        let cfg = WiktionaryParseCfg::default_v1();
        let txt = "==English==\n===Noun===\n# A {{lb|en|slang}} test sense\n";
        let got = parse_wiktionary_page_text("x", txt, cfg).unwrap();
        assert_eq!(got.senses, vec!["A {{lb|en|slang}} test sense".to_string()]);
    }

    #[test]
    fn parse_sense_keeps_malformed_lb_template() {
        let cfg = WiktionaryParseCfg::default_v1();
        let txt = "==English==\n===Noun===\n# {{lb|en|slang} A test sense\n";
        let got = parse_wiktionary_page_text("x", txt, cfg).unwrap();
        assert_eq!(got.senses, vec!["{{lb|en|slang} A test sense".to_string()]);
    }
    #[test]
    fn parse_relation_targets_wikilink_with_label_extracts_target() {
        let cfg = WiktionaryParseCfg::default_v1();
        let txt = "==English==
===Noun===
# A sense
====Synonyms====
* [[target|label]]
";
        let got = parse_wiktionary_page_text("x", txt, cfg).unwrap();
        assert_eq!(got.synonyms, vec!["target".to_string()]);
    }

    #[test]
    fn parse_relation_targets_wikilink_with_multiple_pipes_extracts_first() {
        let cfg = WiktionaryParseCfg::default_v1();
        let txt = "==English==
===Noun===
# A sense
====Synonyms====
* [[word|display|ignored]]
";
        let got = parse_wiktionary_page_text("x", txt, cfg).unwrap();
        assert_eq!(got.synonyms, vec!["word".to_string()]);
    }

    #[test]
    fn parse_relation_targets_m_template_extracts_target() {
        let cfg = WiktionaryParseCfg::default_v1();
        let txt = "==English==
===Noun===
# A sense
====Synonyms====
* {{m|en|baz}}
";
        let got = parse_wiktionary_page_text("x", txt, cfg).unwrap();
        assert_eq!(got.synonyms, vec!["baz".to_string()]);
    }

    #[test]
    fn parse_extended_pos_mask() {
        let cfg = WiktionaryParseCfg::default_v1();
        let txt = "==English==\n===Pronoun===\n# A pronoun\n===Determiner===\n# A determiner\n===Preposition===\n# A preposition\n===Conjunction===\n# A conjunction\n===Interjection===\n# An interjection\n===Numeral===\n# A numeral\n===Particle===\n# A particle\n";
        let got = parse_wiktionary_page_text("x", txt, cfg).unwrap();
        let want = POS_PRONOUN
            | POS_DETERMINER
            | POS_PREPOSITION
            | POS_CONJUNCTION
            | POS_INTERJECTION
            | POS_NUMERAL
            | POS_PARTICLE;
        assert_eq!(got.pos_mask, want);
        assert_eq!(got.senses.len(), 7);
    }

    #[test]
    fn parse_extended_relation_headings() {
        let cfg = WiktionaryParseCfg::default_v1();
        let txt = "==English==\n===Noun===\n# A sense\n====Derived terms====\n* [[alpha]]\n====Coordinate terms====\n* [[beta]]\n====Holonyms====\n* [[gamma]]\n====Meronyms====\n* [[delta]]\n";
        let got = parse_wiktionary_page_text("x", txt, cfg).unwrap();
        assert_eq!(got.pos_mask, POS_NOUN);
        assert_eq!(got.derived_terms, vec!["alpha".to_string()]);
        assert_eq!(got.coordinate_terms, vec!["beta".to_string()]);
        assert_eq!(got.holonyms, vec!["gamma".to_string()]);
        assert_eq!(got.meronyms, vec!["delta".to_string()]);
    }

    #[test]
    fn parse_heading_with_spaces() {
        assert_eq!(parse_heading("== English =="), Some((2, "English")));
        assert_eq!(parse_heading("===Proper noun==="), Some((3, "Proper noun")));
        assert_eq!(
            parse_heading("====Related terms===="),
            Some((4, "Related terms"))
        );
    }
}
