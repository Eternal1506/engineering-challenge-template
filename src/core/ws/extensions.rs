//! The server's answer to our extension offer (RFC 7692), read strictly:
//! permessage-deflate only, only if offered, with only the parameters a
//! server may send back to an offer that names no window sizes.

use std::fmt;

use anyhow::{Context, bail, ensure};

/// The one extension we offer.
pub(super) const PERMESSAGE_DEFLATE: &str = "permessage-deflate";

/// The permessage-deflate terms a server agreed to.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct Deflate {
    server_no_context_takeover: bool,
    client_no_context_takeover: bool,
    server_max_window_bits: Option<u8>,
}

impl fmt::Display for Deflate {
    /// The canonical `Sec-WebSocket-Extensions` value for these terms.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(PERMESSAGE_DEFLATE)?;
        if self.server_no_context_takeover {
            f.write_str("; server_no_context_takeover")?;
        }
        if self.client_no_context_takeover {
            f.write_str("; client_no_context_takeover")?;
        }
        match self.server_max_window_bits {
            Some(bits) => write!(f, "; server_max_window_bits={bits}"),
            None => Ok(()),
        }
    }
}

/// Reads the `Sec-WebSocket-Extensions` lines of an answer, given whether
/// permessage-deflate was `offered`. Several lines read as one list. Fails,
/// naming the value, on anything but at most one permessage-deflate that
/// was offered.
pub(super) fn agreed(lines: &[&str], offered: bool) -> anyhow::Result<Option<Deflate>> {
    if lines.is_empty() {
        return Ok(None);
    }
    let value = lines.join(", ");
    let deflate = parse_list(&value, offered)
        .with_context(|| format!("unacceptable Sec-WebSocket-Extensions {value:?}"))?;
    Ok(Some(deflate))
}

fn parse_list(value: &str, offered: bool) -> anyhow::Result<Deflate> {
    let mut extensions = value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty());
    let extension = extensions.next().context("no extension named")?;
    ensure!(extensions.next().is_none(), "more than one extension");
    let mut parts = extension.split(';').map(str::trim);
    let name = parts.next().unwrap_or_default();
    ensure!(
        name.eq_ignore_ascii_case(PERMESSAGE_DEFLATE),
        "not permessage-deflate"
    );
    ensure!(offered, "permessage-deflate was not offered");
    let mut deflate = Deflate::default();
    for parameter in parts {
        deflate.apply(parameter)?;
    }
    Ok(deflate)
}

impl Deflate {
    /// Takes one `name[=value]` parameter, once at most.
    fn apply(&mut self, parameter: &str) -> anyhow::Result<()> {
        let (name, value) = match parameter.split_once('=') {
            Some((name, value)) => (name.trim(), Some(unquote(value.trim()))),
            None => (parameter, None),
        };
        match (name.to_ascii_lowercase().as_str(), value) {
            ("server_no_context_takeover", None) => set_once(&mut self.server_no_context_takeover),
            ("client_no_context_takeover", None) => set_once(&mut self.client_no_context_takeover),
            ("server_max_window_bits", Some(bits)) => self.set_window_bits(bits),
            _ => bail!("unexpected parameter {parameter:?}"),
        }
    }

    fn set_window_bits(&mut self, bits: &str) -> anyhow::Result<()> {
        ensure!(
            self.server_max_window_bits.is_none(),
            "server_max_window_bits twice"
        );
        let bits: u8 = bits
            .parse()
            .ok()
            .filter(|bits| (8..=15).contains(bits))
            .with_context(|| format!("server_max_window_bits {bits:?} is not 8 to 15"))?;
        self.server_max_window_bits = Some(bits);
        Ok(())
    }
}

fn set_once(flag: &mut bool) -> anyhow::Result<()> {
    ensure!(!*flag, "a parameter given twice");
    *flag = true;
    Ok(())
}

/// `value` without one pair of surrounding double quotes.
fn unquote(value: &str) -> &str {
    value
        .strip_prefix('"')
        .and_then(|inner| inner.strip_suffix('"'))
        .unwrap_or(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(lines: &[&str]) -> anyhow::Result<Option<String>> {
        Ok(agreed(lines, true)?.map(|deflate| deflate.to_string()))
    }

    fn rejects(lines: &[&str]) -> String {
        let error = read(lines).expect_err("should be rejected");
        format!("{error:#}")
    }

    #[test]
    fn no_answer_agrees_to_nothing() {
        assert_eq!(read(&[]).unwrap(), None);
    }

    #[test]
    fn kalshis_answer_is_accepted() {
        let agreed = read(&["permessage-deflate; client_no_context_takeover"]).unwrap();
        assert_eq!(
            agreed.as_deref(),
            Some("permessage-deflate; client_no_context_takeover")
        );
    }

    #[test]
    fn a_bare_answer_is_accepted() {
        let agreed = read(&["permessage-deflate"]).unwrap();
        assert_eq!(agreed.as_deref(), Some("permessage-deflate"));
    }

    #[test]
    fn every_allowed_parameter_is_kept_in_canonical_form() {
        let answer = "Permessage-Deflate;server_max_window_bits=\"12\" ; \
                      CLIENT_NO_CONTEXT_TAKEOVER; server_no_context_takeover";
        let canonical = "permessage-deflate; server_no_context_takeover; \
                         client_no_context_takeover; server_max_window_bits=12";
        assert_eq!(read(&[answer]).unwrap().as_deref(), Some(canonical));
    }

    #[test]
    fn window_bits_from_8_to_15_are_accepted() {
        for bits in [8, 15] {
            let answer = format!("permessage-deflate; server_max_window_bits={bits}");
            let expected = answer.clone();
            assert_eq!(read(&[&answer]).unwrap(), Some(expected));
        }
    }

    #[test]
    fn window_bits_outside_8_to_15_are_rejected() {
        for bits in ["7", "16", "", "x"] {
            let answer = format!("permessage-deflate; server_max_window_bits={bits}");
            assert!(rejects(&[&answer]).contains("8 to 15"), "{bits}");
        }
    }

    #[test]
    fn server_max_window_bits_without_a_value_is_rejected() {
        let error = rejects(&["permessage-deflate; server_max_window_bits"]);
        assert!(error.contains("unexpected parameter"), "{error}");
    }

    #[test]
    fn client_max_window_bits_is_rejected_as_not_offered() {
        let error = rejects(&["permessage-deflate; client_max_window_bits=10"]);
        assert!(error.contains("client_max_window_bits"), "{error}");
    }

    #[test]
    fn an_unknown_parameter_is_rejected() {
        let error = rejects(&["permessage-deflate; mystery"]);
        assert!(error.contains("mystery"), "{error}");
    }

    #[test]
    fn a_repeated_parameter_is_rejected() {
        rejects(&["permessage-deflate; client_no_context_takeover; client_no_context_takeover"]);
        rejects(&["permessage-deflate; server_max_window_bits=9; server_max_window_bits=10"]);
    }

    #[test]
    fn another_extension_is_rejected() {
        let error = rejects(&["x-webkit-deflate-frame"]);
        assert!(error.contains("x-webkit-deflate-frame"), "{error}");
    }

    #[test]
    fn two_extensions_in_one_line_are_rejected() {
        let error = rejects(&["permessage-deflate, permessage-deflate"]);
        assert!(error.contains("more than one"), "{error}");
    }

    #[test]
    fn two_lines_are_one_list() {
        let error = rejects(&["permessage-deflate", "permessage-deflate"]);
        assert!(error.contains("more than one"), "{error}");
        let agreed = read(&["permessage-deflate", ""]).unwrap();
        assert_eq!(agreed.as_deref(), Some("permessage-deflate"));
    }

    #[test]
    fn an_empty_answer_is_rejected() {
        assert!(rejects(&[""]).contains("no extension"));
    }

    #[test]
    fn deflate_that_was_not_offered_is_rejected() {
        let error = agreed(&["permessage-deflate"], false).unwrap_err();
        assert!(format!("{error:#}").contains("not offered"), "{error:#}");
    }
}
