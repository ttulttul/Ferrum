//! Whether a shell command line is plainly read-only, so that Antigravity
//! may run it in Auto-accept edits without asking.
//!
//! This is a deliberate difference from the reference, which asks about
//! every command in Auto-accept edits (user decision: read-only commands
//! shouldn't block Antigravity in that mode).
//!
//! The check is an explicit allowlist, not a heuristic. A command line is
//! read-only only when it is a single pipeline of simple invocations, each
//! of an allowlisted program with no argument that lets it write or run
//! something else. Anything the parser does not plainly understand
//! (redirection, chaining, substitution, expansion, variables, environment
//! assignments, escapes, an unterminated quote) makes it ask.

/// Programs that only read, whatever their arguments, apart from those
/// [`forbidden_argument`] refuses.
const READ_ONLY_PROGRAMS: &[&str] = &[
    "basename", "cat", "cmp", "comm", "cut", "date", "df", "diff", "dirname", "du", "egrep",
    "fgrep", "file", "find", "git", "grep", "head", "id", "ls", "nl", "pwd", "readlink",
    "realpath", "rg", "sort", "stat", "tail", "tr", "tree", "uname", "wc", "which", "whoami",
];

/// Git subcommands that only read the repository.
const READ_ONLY_GIT: &[&str] = &[
    "blame",
    "diff",
    "log",
    "ls-files",
    "rev-parse",
    "show",
    "status",
];

/// Whether `command` is a plainly read-only command line.
pub(crate) fn is_read_only(command: &str) -> bool {
    let Some(pipeline) = split_pipeline(command) else {
        return false;
    };
    !pipeline.is_empty() && pipeline.iter().all(|words| invocation_reads_only(words))
}

/// The command line's pipeline as words, or `None` when it holds anything
/// but quoted or plain words separated by single `|`s.
fn split_pipeline(command: &str) -> Option<Vec<Vec<String>>> {
    let mut pipeline = Vec::new();
    let mut words: Vec<String> = Vec::new();
    let mut word = String::new();
    // A word has started (so `''` is an empty word, not nothing).
    let mut in_word = false;
    let mut chars = command.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            ' ' | '\t' => {
                if in_word {
                    words.push(std::mem::take(&mut word));
                    in_word = false;
                }
            }
            '|' => {
                // `||` chains; `|&` pipes stderr too: both ask.
                if matches!(chars.peek(), Some('|' | '&')) {
                    return None;
                }
                if in_word {
                    words.push(std::mem::take(&mut word));
                    in_word = false;
                }
                if words.is_empty() {
                    return None;
                }
                pipeline.push(std::mem::take(&mut words));
            }
            '\'' => {
                // Literal up to the next quote.
                in_word = true;
                loop {
                    match chars.next() {
                        Some('\'') => break,
                        Some(c) => word.push(c),
                        None => return None,
                    }
                }
            }
            '"' => {
                // No expansion or escape inside: any would ask.
                in_word = true;
                loop {
                    match chars.next() {
                        Some('"') => break,
                        Some('$' | '`' | '\\' | '!') | None => return None,
                        Some(c) => word.push(c),
                    }
                }
            }
            // Chaining, redirection, substitution, expansion, grouping,
            // escapes, comments, history and line breaks all ask.
            ';' | '&' | '<' | '>' | '$' | '`' | '\\' | '(' | ')' | '{' | '}' | '#' | '!' | '\n'
            | '\r' => return None,
            c if c.is_control() => return None,
            c => {
                in_word = true;
                word.push(c);
            }
        }
    }
    if in_word {
        words.push(word);
    }
    if words.is_empty() {
        // An empty command line, or one ending in a pipe.
        return None;
    }
    pipeline.push(words);
    Some(pipeline)
}

/// Whether one simple invocation only reads.
fn invocation_reads_only(words: &[String]) -> bool {
    let Some((program, args)) = words.split_first() else {
        return false;
    };
    // A path (`/bin/ls`, `./ls`) or an environment assignment (`A=b ls`)
    // is not a plain allowlisted name.
    if !READ_ONLY_PROGRAMS.contains(&program.as_str()) {
        return false;
    }
    if args.iter().any(|arg| forbidden_argument(program, arg)) {
        return false;
    }
    match program.as_str() {
        "git" => git_reads_only(args),
        _ => true,
    }
}

/// An argument with which `program` may write, run another program or
/// open an editor.
fn forbidden_argument(program: &str, arg: &str) -> bool {
    let starts = |prefixes: &[&str]| prefixes.iter().any(|p| arg.starts_with(p));
    match program {
        "find" => starts(&["-exec", "-ok", "-delete", "-fprint", "-fls"]),
        "sort" => {
            // `-o FILE` (also bundled, as `-uo`), `--output`, and the
            // compressor sort may run.
            (arg.starts_with('-') && !arg.starts_with("--") && arg.contains('o'))
                || starts(&["--output", "--compress-program"])
        }
        "rg" => starts(&["--pre"]),
        "git" => {
            // Writes a file, runs an external diff or a pager/editor.
            starts(&[
                "--output",
                "--ext-diff",
                "--open-files-in-pager",
                "--exec",
                "-O",
            ])
        }
        "tree" => starts(&["-o"]),
        // `-C` compiles a magic file, writing it.
        "file" => starts(&["-C", "--compile"]),
        "date" => starts(&["-s", "--set"]),
        _ => false,
    }
}

/// Whether a `git` invocation's subcommand only reads. Global options
/// before it are refused, except `--no-pager` and `-C <dir>`.
fn git_reads_only(args: &[String]) -> bool {
    let mut rest = args.iter();
    while let Some(arg) = rest.next() {
        match arg.as_str() {
            "--no-pager" => {}
            "-C" => {
                if rest.next().is_none() {
                    return false;
                }
            }
            sub if READ_ONLY_GIT.contains(&sub) => return true,
            _ => return false,
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::is_read_only;

    #[test]
    fn allowlisted_invocations_read_only() {
        for command in [
            "ls",
            "ls -la /workspace",
            "  ls -1 notes  ",
            "cat README.md",
            "head -n 20 src/main.rs",
            "tail -n 5 log.txt",
            "wc -l a b",
            "pwd",
            "grep -rn 'fn main' src",
            "grep -o '<h1>.*</h1>' index.html",
            "rg --files",
            "rg -n \"todo\" src",
            "git status",
            "git status --short",
            "git log --oneline -5",
            "git diff HEAD~1",
            "git show HEAD:README.md",
            "git -C /workspace status",
            "git --no-pager log",
            "find . -name '*.rs' -type f",
            "ls -1 /workspace | wc -l",
            "cat a.txt | grep x | sort | head",
            "sort -r names.txt",
            "grep 'a\\.b' file",
            "ls ''",
        ] {
            assert!(is_read_only(command), "{command}");
        }
    }

    #[test]
    fn writes_and_unknown_programs_ask() {
        for command in [
            "touch x",
            "rm x",
            "mkdir d",
            "tee out.txt",
            "sed -i s/a/b/ f",
            "python3 -c 'print(1)'",
            "sh",
            "bash -c ls",
            "/bin/ls",
            "./ls",
            "xargs cat",
            "env ls",
            "sudo ls",
            "echo hi",
            "uniq in out",
            "git commit -m x",
            "git checkout main",
            "git branch new",
            "git stash",
            "git push",
            "git diff --output=patch.diff",
            "git diff --output patch.diff",
            "git log --ext-diff",
            "git grep -O x",
            "git -c core.pager=sh log",
            "git",
            "git -C",
            "git -C /workspace",
            "sort -o out.txt in.txt",
            "sort -uo out.txt in.txt",
            "sort --output=out.txt in.txt",
            "sort --compress-program=sh in.txt",
            "rg --pre ./script x",
            "find . -exec rm {} +",
            "find . -execdir rm '{}' ';'",
            "find . -delete",
            "find . -ok rm",
            "find . -fprint out",
            "find . -fprintf out %p",
            "find . -fls out",
            "tree -o out.txt",
            "file -C -m magic",
            "date -s 2020-01-01",
        ] {
            assert!(!is_read_only(command), "{command}");
        }
    }

    #[test]
    fn redirections_chains_and_substitutions_ask() {
        for command in [
            "ls > f",
            "ls >> f",
            "ls 2>/dev/null",
            "ls >| f",
            "cat < f",
            "cat <<EOF",
            "ls &> f",
            "ls; rm x",
            "ls && rm x",
            "ls || rm x",
            "ls & rm x",
            "ls &",
            "ls | sh",
            "ls | xargs rm",
            "cat f | tee g",
            "ls |& cat",
            "ls |",
            "| ls",
            "ls || ",
            "cat $(echo f)",
            "cat `echo f`",
            "cat \"$(echo f)\"",
            "cat \"`echo f`\"",
            "cat $HOME/f",
            "cat ${HOME}",
            "cat \"$HOME\"",
            "diff <(ls a) <(ls b)",
            "(ls)",
            "{ ls; }",
            "A=1 ls",
            "PAGER=sh git log",
            "ls \\; rm x",
            "cat 'unterminated",
            "cat \"unterminated",
            "ls # comment",
            "ls\nrm x",
            "ls\rrm x",
            "!!",
            "",
            "   ",
        ] {
            assert!(!is_read_only(command), "{command:?}");
        }
    }
}
