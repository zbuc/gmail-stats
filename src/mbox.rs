//! Streaming, header-only parser for mbox files (what Google Takeout exports).
//!
//! Design constraints, in service of multi-GB archives:
//!
//! * **Bounded memory**: the file is read through a `BufRead` one line at a
//!   time into a single reusable buffer. Lines longer than [`MAX_LINE_BYTES`]
//!   are truncated in memory but always fully consumed from the stream, so
//!   message framing survives pathological input. Only the three headers the
//!   importer needs (`From`, `Return-Path`, `Message-ID`) are retained, each
//!   capped at [`MAX_HEADER_VALUE_BYTES`] after unfolding.
//! * **Header-only**: bodies are skipped line-by-line without interpretation.
//! * **mboxrd `>From ` escaping**: Takeout escapes body lines that would look
//!   like separators as `>From `; such lines do not match the `From ` prefix
//!   and are naturally treated as body content. An *unescaped* `From ` line
//!   is, per the format, a new message separator.
//! * **CRLF**: a trailing `\r` is stripped from every line, so CRLF archives
//!   parse identically to LF ones (offsets still count the real bytes).
//! * **Malformed regions**: garbage before the first separator, or a header
//!   section containing a line that is neither a header, a continuation, nor
//!   a separator, causes a skip to the next valid `From ` line. Each skipped
//!   region increments [`MboxReader::skipped`]; the caller additionally counts
//!   messages without a `Message-ID` as skipped.
//! * **Resumability**: every returned message carries `end_offset`, the
//!   absolute offset of the byte after the message (i.e. the start of the next
//!   `From ` separator, or EOF). A reader constructed with that offset over a
//!   file seeked to it continues the parse exactly.

use std::io::{self, BufRead};

/// Longest raw line kept in memory. Longer lines are truncated (but fully
/// consumed). Sender and Message-ID headers fit comfortably within this.
const MAX_LINE_BYTES: usize = 8 * 1024;
/// Cap on an unfolded (multi-line) header value.
const MAX_HEADER_VALUE_BYTES: usize = 8 * 1024;

/// The handful of headers the importer cares about, parsed out of one message.
#[derive(Debug)]
pub struct RawMessage {
    pub message_id: Option<String>,
    pub from: Option<String>,
    pub return_path: Option<String>,
    /// Absolute offset of the first byte after this message: the start of the
    /// next message's `From ` separator line, or EOF. Once every message up to
    /// here has been committed, this is a safe byte offset to resume from.
    pub end_offset: u64,
}

#[derive(Clone, Copy, Debug)]
enum Interest {
    From,
    ReturnPath,
    MessageId,
}

/// The header currently being accumulated (headers may be folded over
/// multiple lines, so a header is only committed when the next one starts).
enum Current {
    /// Not inside any header yet (start of the header section).
    None,
    /// Inside a header the importer does not care about.
    Boring,
    /// Inside one of the interesting headers.
    Keep(Interest, String),
}

pub struct MboxReader<R: BufRead> {
    inner: R,
    /// Absolute offset of the next unread byte.
    offset: u64,
    /// Reusable line buffer (capped at MAX_LINE_BYTES).
    line: Vec<u8>,
    /// The next message's `From ` separator line was already consumed while
    /// scanning for the end of the previous message.
    pending_from: bool,
    /// Malformed regions skipped so far.
    skipped: u64,
}

impl<R: BufRead> MboxReader<R> {
    /// `start_offset` is the absolute file offset the reader `inner` is
    /// positioned at (0 for a whole file, or a resume offset for a seeked
    /// file); all reported offsets are absolute.
    pub fn new(inner: R, start_offset: u64) -> Self {
        MboxReader {
            inner,
            offset: start_offset,
            line: Vec::with_capacity(1024),
            pending_from: false,
            skipped: 0,
        }
    }

    /// Number of malformed regions skipped so far (junk before a separator, or
    /// a header section that could not be parsed).
    pub fn skipped(&self) -> u64 {
        self.skipped
    }

    fn read_line(&mut self) -> io::Result<u64> {
        read_line_capped(&mut self.inner, &mut self.line)
    }

    /// Parse the next message's headers; `Ok(None)` at EOF.
    pub fn next_message(&mut self) -> io::Result<Option<RawMessage>> {
        'message: loop {
            if !self.at_separator()? {
                return Ok(None);
            }

            let mut msg = RawMessage {
                message_id: None,
                from: None,
                return_path: None,
                end_offset: 0,
            };
            let mut current = Current::None;

            // Header section: runs until a blank line (body follows), a new
            // `From ` separator (message without a body), or EOF.
            loop {
                let n = self.read_line()?;
                if n == 0 {
                    commit(&mut current, &mut msg);
                    msg.end_offset = self.offset;
                    return Ok(Some(msg));
                }
                self.offset += n;

                if self.line.is_empty() {
                    commit(&mut current, &mut msg);
                    // Body: skip lines until the next separator or EOF. An
                    // mboxrd-escaped `>From ` line does not match the prefix
                    // and is skipped like any other body line.
                    loop {
                        let n = self.read_line()?;
                        if n == 0 {
                            msg.end_offset = self.offset;
                            return Ok(Some(msg));
                        }
                        self.offset += n;
                        if self.line.starts_with(b"From ") {
                            self.pending_from = true;
                            msg.end_offset = self.offset - n;
                            return Ok(Some(msg));
                        }
                    }
                }

                if self.line.starts_with(b"From ") {
                    // Message with no blank line / body before the next one.
                    commit(&mut current, &mut msg);
                    self.pending_from = true;
                    msg.end_offset = self.offset - n;
                    return Ok(Some(msg));
                }

                if self.line[0] == b' ' || self.line[0] == b'\t' {
                    // Folded continuation of the previous header line.
                    match &mut current {
                        Current::None => {
                            // Continuation with no header before it: malformed.
                            self.skipped += 1;
                            self.seek_next_separator()?;
                            continue 'message;
                        }
                        Current::Boring => {}
                        Current::Keep(_, value) => {
                            let folded = String::from_utf8_lossy(&self.line);
                            let folded = folded.trim();
                            if !folded.is_empty()
                                && value.len() + folded.len() < MAX_HEADER_VALUE_BYTES
                            {
                                if !value.is_empty() {
                                    value.push(' ');
                                }
                                value.push_str(folded);
                            }
                        }
                    }
                    continue;
                }

                match self.line.iter().position(|&b| b == b':') {
                    None | Some(0) => {
                        // Not a header, a continuation, or a separator: give
                        // up on this message and skip to the next one.
                        self.skipped += 1;
                        self.seek_next_separator()?;
                        continue 'message;
                    }
                    Some(colon) => {
                        commit(&mut current, &mut msg);
                        let name = self.line[..colon].trim_ascii();
                        current = match interest_for(name) {
                            Some(interest) => {
                                let value = String::from_utf8_lossy(&self.line[colon + 1..])
                                    .trim()
                                    .to_string();
                                Current::Keep(interest, value)
                            }
                            None => Current::Boring,
                        };
                    }
                }
            }
        }
    }

    /// Position the parser just after a `From ` separator line. Returns false
    /// at EOF. Counts any non-blank junk encountered on the way as one
    /// skipped region.
    fn at_separator(&mut self) -> io::Result<bool> {
        if self.pending_from {
            self.pending_from = false;
            return Ok(true);
        }
        let mut saw_junk = false;
        loop {
            let n = self.read_line()?;
            if n == 0 {
                if saw_junk {
                    self.skipped += 1;
                }
                return Ok(false);
            }
            self.offset += n;
            if self.line.starts_with(b"From ") {
                if saw_junk {
                    self.skipped += 1;
                }
                return Ok(true);
            }
            if !self.line.is_empty() {
                saw_junk = true;
            }
        }
    }

    /// After a malformed region: discard lines until the next `From `
    /// separator (left pending) or EOF.
    fn seek_next_separator(&mut self) -> io::Result<()> {
        loop {
            let n = self.read_line()?;
            if n == 0 {
                return Ok(());
            }
            self.offset += n;
            if self.line.starts_with(b"From ") {
                self.pending_from = true;
                return Ok(());
            }
        }
    }
}

fn commit(current: &mut Current, msg: &mut RawMessage) {
    if let Current::Keep(interest, value) = std::mem::replace(current, Current::None) {
        let slot = match interest {
            Interest::From => &mut msg.from,
            Interest::ReturnPath => &mut msg.return_path,
            Interest::MessageId => &mut msg.message_id,
        };
        // First occurrence wins; empty values are as good as absent.
        if slot.is_none() && !value.is_empty() {
            *slot = Some(value);
        }
    }
}

fn interest_for(name: &[u8]) -> Option<Interest> {
    if name.eq_ignore_ascii_case(b"from") {
        Some(Interest::From)
    } else if name.eq_ignore_ascii_case(b"return-path") {
        Some(Interest::ReturnPath)
    } else if name.eq_ignore_ascii_case(b"message-id") {
        Some(Interest::MessageId)
    } else {
        None
    }
}

/// Read one line (through the next `\n` or EOF) into `buf`, which is cleared
/// first. At most [`MAX_LINE_BYTES`] are kept, but the full line is always
/// consumed from the stream. The newline is not stored; a trailing `\r` is
/// stripped. Returns the number of bytes consumed; 0 means EOF.
fn read_line_capped<R: BufRead>(reader: &mut R, buf: &mut Vec<u8>) -> io::Result<u64> {
    buf.clear();
    let mut consumed: u64 = 0;
    loop {
        let (done, used) = {
            let available = match reader.fill_buf() {
                Ok(a) => a,
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            };
            if available.is_empty() {
                break;
            }
            match available.iter().position(|&b| b == b'\n') {
                Some(i) => {
                    if buf.len() < MAX_LINE_BYTES {
                        let end = i.min(MAX_LINE_BYTES - buf.len());
                        buf.extend_from_slice(&available[..end]);
                    }
                    (true, i + 1)
                }
                None => {
                    if buf.len() < MAX_LINE_BYTES {
                        let end = available.len().min(MAX_LINE_BYTES - buf.len());
                        buf.extend_from_slice(&available[..end]);
                    }
                    (false, available.len())
                }
            }
        };
        reader.consume(used);
        consumed += used as u64;
        if done {
            break;
        }
    }
    if buf.last() == Some(&b'\r') {
        buf.pop();
    }
    Ok(consumed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufReader, Cursor, Read, Write};

    fn read_all(data: &[u8]) -> (Vec<RawMessage>, u64) {
        let mut reader = MboxReader::new(Cursor::new(data), 0);
        let mut messages = Vec::new();
        while let Some(msg) = reader.next_message().unwrap() {
            messages.push(msg);
        }
        (messages, reader.skipped())
    }

    const TWO_MESSAGES: &str = "From a@example.com Thu Jan  1 00:00:00 2020\n\
        From: Alice <a@example.com>\n\
        Message-ID: <m1@example.com>\n\
        Subject: hello\n\
        \n\
        body line one\n\
        body line two\n\
        From b@example.com Thu Jan  1 00:00:00 2020\n\
        Return-Path: <bounce@example.com>\n\
        Message-ID: <m2@example.com>\n\
        \n\
        body\n";

    #[test]
    fn parses_normal_messages() {
        let (msgs, skipped) = read_all(TWO_MESSAGES.as_bytes());
        assert_eq!(skipped, 0);
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].from.as_deref(), Some("Alice <a@example.com>"));
        assert_eq!(msgs[0].message_id.as_deref(), Some("<m1@example.com>"));
        assert_eq!(msgs[1].from, None);
        assert_eq!(msgs[1].return_path.as_deref(), Some("<bounce@example.com>"));
        assert_eq!(msgs[1].message_id.as_deref(), Some("<m2@example.com>"));
        // end_offset of a message is the start of the next separator / EOF.
        let second_start = TWO_MESSAGES.find("From b@").unwrap() as u64;
        assert_eq!(msgs[0].end_offset, second_start);
        assert_eq!(msgs[1].end_offset, TWO_MESSAGES.len() as u64);
    }

    #[test]
    fn parses_crlf_line_endings() {
        let crlf = TWO_MESSAGES.replace('\n', "\r\n");
        let (msgs, skipped) = read_all(crlf.as_bytes());
        assert_eq!(skipped, 0);
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].from.as_deref(), Some("Alice <a@example.com>"));
        assert_eq!(msgs[1].message_id.as_deref(), Some("<m2@example.com>"));
        // Offsets count the real (CRLF) bytes.
        assert_eq!(msgs[1].end_offset, crlf.len() as u64);
    }

    #[test]
    fn mboxrd_escaped_from_lines_stay_in_the_body() {
        let data = "From a@example.com Thu Jan  1 00:00:00 2020\n\
            From: a@example.com\n\
            Message-ID: <m1@example.com>\n\
            \n\
            >From the archives, an escaped line\n\
            >>From twice-escaped\n\
            still message one\n";
        let (msgs, skipped) = read_all(data.as_bytes());
        assert_eq!(skipped, 0);
        assert_eq!(msgs.len(), 1, "escaped From lines must not split messages");
        assert_eq!(msgs[0].end_offset, data.len() as u64);
    }

    #[test]
    fn unescaped_from_line_in_body_is_a_separator() {
        // mboxrd semantics: an unescaped `From ` line *is* a separator. The
        // resulting bogus "message" has no parseable headers and is counted
        // as a skipped region.
        let data = "From a@example.com Thu Jan  1 00:00:00 2020\n\
            From: a@example.com\n\
            Message-ID: <m1@example.com>\n\
            \n\
            From here on, an unescaped body line\n\
            more body\n";
        let (msgs, skipped) = read_all(data.as_bytes());
        assert_eq!(msgs.len(), 1);
        assert_eq!(skipped, 1);
    }

    #[test]
    fn missing_message_id_is_reported_as_none() {
        let data = "From a@example.com Thu Jan  1 00:00:00 2020\n\
            From: a@example.com\n\
            Subject: no message id\n\
            \n\
            body\n";
        let (msgs, skipped) = read_all(data.as_bytes());
        assert_eq!(skipped, 0);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].message_id, None);
        assert_eq!(msgs[0].from.as_deref(), Some("a@example.com"));
    }

    #[test]
    fn malformed_header_region_skips_to_next_separator() {
        let data = "From a@example.com Thu Jan  1 00:00:00 2020\n\
            From: a@example.com\n\
            this line is not a header and not a continuation\n\
            trailing garbage\n\
            From b@example.com Thu Jan  1 00:00:00 2020\n\
            From: b@example.com\n\
            Message-ID: <m2@example.com>\n\
            \n\
            body\n";
        let (msgs, skipped) = read_all(data.as_bytes());
        assert_eq!(skipped, 1);
        assert_eq!(msgs.len(), 1, "the malformed message is dropped");
        assert_eq!(msgs[0].message_id.as_deref(), Some("<m2@example.com>"));
    }

    #[test]
    fn junk_before_first_separator_is_skipped_and_counted() {
        let data = "random junk that is not mbox\nmore junk\n\
            From a@example.com Thu Jan  1 00:00:00 2020\n\
            From: a@example.com\n\
            Message-ID: <m1@example.com>\n\
            \n\
            body\n";
        let (msgs, skipped) = read_all(data.as_bytes());
        assert_eq!(skipped, 1);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].message_id.as_deref(), Some("<m1@example.com>"));
    }

    #[test]
    fn folded_headers_are_unfolded() {
        let data = "From a@example.com Thu Jan  1 00:00:00 2020\n\
            From: A Very Long Display Name\n\
            \t<folded@example.com>\n\
            Message-ID:\n\
            \x20<folded-id@example.com>\n\
            \n\
            body\n";
        let (msgs, skipped) = read_all(data.as_bytes());
        assert_eq!(skipped, 0);
        assert_eq!(
            msgs[0].from.as_deref(),
            Some("A Very Long Display Name <folded@example.com>")
        );
        assert_eq!(
            msgs[0].message_id.as_deref(),
            Some("<folded-id@example.com>")
        );
    }

    #[test]
    fn header_names_are_case_insensitive_and_first_wins() {
        let data = "From a@example.com Thu Jan  1 00:00:00 2020\n\
            RETURN-PATH: <first@example.com>\n\
            return-path: <second@example.com>\n\
            MESSAGE-id: <m1@example.com>\n\
            \n\
            body\n";
        let (msgs, _) = read_all(data.as_bytes());
        assert_eq!(msgs[0].return_path.as_deref(), Some("<first@example.com>"));
        assert_eq!(msgs[0].message_id.as_deref(), Some("<m1@example.com>"));
    }

    #[test]
    fn overlong_lines_are_truncated_but_framing_survives() {
        let long_value = "a".repeat(3 * MAX_LINE_BYTES);
        let data = format!(
            "From a@example.com Thu Jan  1 00:00:00 2020\n\
             X-Long: {long_value}\n\
             Message-ID: <m1@example.com>\n\
             \n\
             {long_value}\n\
             From b@example.com Thu Jan  1 00:00:00 2020\n\
             Message-ID: <m2@example.com>\n\
             \n\
             body\n"
        );
        let (msgs, skipped) = read_all(data.as_bytes());
        assert_eq!(skipped, 0);
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].message_id.as_deref(), Some("<m1@example.com>"));
        assert_eq!(msgs[1].message_id.as_deref(), Some("<m2@example.com>"));
        assert_eq!(msgs[1].end_offset, data.len() as u64);
    }

    #[test]
    fn message_without_body_or_trailing_newline() {
        let data = "From a@example.com Thu Jan  1 00:00:00 2020\n\
            From: a@example.com\n\
            Message-ID: <m1@example.com>";
        let (msgs, skipped) = read_all(data.as_bytes());
        assert_eq!(skipped, 0);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].message_id.as_deref(), Some("<m1@example.com>"));
        assert_eq!(msgs[0].end_offset, data.len() as u64);
    }

    #[test]
    fn separator_directly_after_headers_ends_the_message() {
        let data = "From a@example.com Thu Jan  1 00:00:00 2020\n\
            Message-ID: <m1@example.com>\n\
            From b@example.com Thu Jan  1 00:00:00 2020\n\
            Message-ID: <m2@example.com>\n\
            \n\
            body\n";
        let (msgs, skipped) = read_all(data.as_bytes());
        assert_eq!(skipped, 0);
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].message_id.as_deref(), Some("<m1@example.com>"));
        assert_eq!(msgs[1].message_id.as_deref(), Some("<m2@example.com>"));
    }

    #[test]
    fn resume_from_end_offset_continues_the_parse() {
        let (all, _) = read_all(TWO_MESSAGES.as_bytes());
        let offset = all[0].end_offset;
        let rest = &TWO_MESSAGES.as_bytes()[offset as usize..];
        let mut reader = MboxReader::new(Cursor::new(rest), offset);
        let msg = reader.next_message().unwrap().unwrap();
        assert_eq!(msg.message_id.as_deref(), Some("<m2@example.com>"));
        assert_eq!(msg.end_offset, TWO_MESSAGES.len() as u64);
        assert!(reader.next_message().unwrap().is_none());
        assert_eq!(reader.skipped(), 0);
    }

    /// An infinite-source style reader: generates mbox messages on the fly, so
    /// the input provably never exists in memory as a whole. Combined with the
    /// capped line buffer this demonstrates the parser streams in bounded
    /// memory regardless of input size.
    struct SyntheticMbox {
        next_index: u64,
        total: u64,
        chunk: Vec<u8>,
        pos: usize,
    }

    fn synthetic_message(i: u64, body_bytes: usize) -> Vec<u8> {
        let body = "x".repeat(72);
        let mut out = format!(
            "From sender{i}@example.com Thu Jan  1 00:00:00 2020\n\
             From: Sender {i} <sender{}@example.com>\n\
             Message-ID: <synthetic-{i}@example.com>\n\
             Subject: synthetic message {i}\n\
             \n",
            i % 997
        )
        .into_bytes();
        let mut written = 0;
        while written < body_bytes {
            out.extend_from_slice(body.as_bytes());
            out.push(b'\n');
            written += body.len() + 1;
        }
        out
    }

    impl Read for SyntheticMbox {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            if self.pos == self.chunk.len() {
                if self.next_index == self.total {
                    return Ok(0);
                }
                self.chunk = synthetic_message(self.next_index, 1024);
                self.pos = 0;
                self.next_index += 1;
            }
            let n = (self.chunk.len() - self.pos).min(out.len());
            out[..n].copy_from_slice(&self.chunk[self.pos..self.pos + n]);
            self.pos += n;
            Ok(n)
        }
    }

    #[test]
    fn streams_a_large_generated_mailbox() {
        // ~64 MB of mbox, generated on the fly (never materialized).
        let total = 55_000u64;
        let source = SyntheticMbox {
            next_index: 0,
            total,
            chunk: Vec::new(),
            pos: 0,
        };
        let mut reader = MboxReader::new(BufReader::with_capacity(64 * 1024, source), 0);
        let mut count = 0u64;
        let mut last_offset = 0u64;
        while let Some(msg) = reader.next_message().unwrap() {
            count += 1;
            assert!(msg.message_id.is_some());
            assert!(msg.end_offset > last_offset);
            last_offset = msg.end_offset;
        }
        assert_eq!(count, total);
        assert_eq!(reader.skipped(), 0);
    }

    #[test]
    fn parses_a_large_mbox_file_from_disk() {
        // ~50 MB real file in a temp dir, parsed through the same streaming
        // path the importer uses (BufReader over File).
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("large.mbox");
        let total = 12_000u64;
        {
            let mut file = std::io::BufWriter::new(std::fs::File::create(&path).unwrap());
            for i in 0..total {
                file.write_all(&synthetic_message(i, 4096)).unwrap();
            }
        }
        let size = std::fs::metadata(&path).unwrap().len();
        assert!(
            size > 45 * 1024 * 1024,
            "fixture should be ~50MB, got {size}"
        );

        let file = std::fs::File::open(&path).unwrap();
        let mut reader = MboxReader::new(BufReader::with_capacity(64 * 1024, file), 0);
        let mut count = 0u64;
        let mut final_offset = 0u64;
        while let Some(msg) = reader.next_message().unwrap() {
            count += 1;
            final_offset = msg.end_offset;
        }
        assert_eq!(count, total);
        assert_eq!(reader.skipped(), 0);
        assert_eq!(final_offset, size);
    }
}
