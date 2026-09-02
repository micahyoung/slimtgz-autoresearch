# slimtgz-autoresearch

A self-improvement loop, modeled on [karpathy/autoresearch](https://github.com/karpathy/autoresearch)
and on this organization's [screencam-autoresearch](../screencam-autoresearch),
for tuning a CLI that recompresses a `.tar.gz` file into a smaller-or-equal
copy that re-extracts to exactly the same content.

`program.md` is the operational briefing a fresh, stateless agent reads at
the start of every round — everything it needs to actually do the work
lives there, and nothing else. This file is the rest: why this exists, how
to run it, and the context a human operating the loop needs that the agent
itself doesn't.

## Files

| File | Owner | Role |
|---|---|---|
| `prepare.go` | fixed, human-only | dataset, round-trip verifier, compute-budget enforcement — the grader |
| `train.go` | agent-edited | the actual pipeline; starts as a plain `gzip.BestCompression` recompress |
| `program.md` | human-authored, agent-read | the in-the-box operational briefing |
| `README.md` | this file | out-of-the-box human context |
| `go.mod` | fixed, human-only | Go module definition; stdlib only unless a round needs more |
| `data/` | fixed | the seven dataset files (see *Dataset provenance* below) |
| `out/` | scratch, gitignored | each round's rendered `$2` outputs |

`prepare.go` refuses to run if `README.md`, `program.md`, or `prepare.go`
itself has uncommitted changes — see *Integrity check* below. This is
deliberate: those three are the fixed, trusted part of the loop, and a
round should never be graded against a dirty, uncommitted version of its
own rules or grader.

## Running a round

```
go run prepare.go
```

This is human-triggered per round, same as screencam-autoresearch — you
decide when the next round happens, review the diff, and decide whether to
keep it per `program.md`'s policy.

Every accepted round is a commit scoped to this directory (`git log` is the
full history — there's no separate results file). Discarded attempts leave
no trace, by design.

## Dependencies

Run everything through `go run`/`go build` (`go run train.go <in> <out>`,
`go run prepare.go`) — no separate package manager step. `go.mod` pins the
module to the Go standard library only; a dependency a round genuinely
needs but that's missing is reported to the human, not added by an agent
round (same protocol as screencam's `pyproject.toml`).

## Integrity check

`prepare.go` runs `git status --porcelain -- README.md program.md
prepare.go` before doing anything else and refuses to proceed if any of the
three show up dirty (modified, staged, or untracked). The guardrail against
reward-hacking in this whole design is "the agent can never touch its own
grader" — that guarantee is worthless if the grader itself can be run in a
modified-but-uncommitted state without anyone noticing. `train.go` is
deliberately excluded from this check; it's supposed to be dirty while a
round is actively working on it.

Practical implication: after editing any of these three files yourself,
commit before running `go run prepare.go` again.

## Compute backpressure, instead of a fixed timeout

karpathy's original harness and screencam-autoresearch both use a fixed
wall-clock cutoff to catch a round that's hung. That doesn't translate well
here: this dataset spans 73KB (`linux001`) to 4.2MB (`alpine-arm64`), so any
single fixed number is either too generous for the small files or too tight
for the large ones. Instead, `prepare.go` measures, in-process, how long an
ordinary `gzip.BestCompression` pass takes over each file's decompressed
content, and gives `train.go` a budget of 100x that number, per file. Going
over budget on any file is a disqualification for the round, not a bad
score — `prepare.go` kills the subprocess and nothing is reported as a
scorable result. See `program.md`'s workflow section for the exact policy.

## Dataset provenance

All seven files were fetched or generated on 2026-09-01. sha256 checksums
below are of the exact bytes committed to `data/`.

| File | Source | sha256 |
|---|---|---|
| `alpine-amd64.tar.gz` | `alpine:latest`, `linux/amd64` — the single OCI filesystem layer blob, pulled via `skopeo copy --override-arch amd64 docker://alpine:latest oci:...` and copied straight out of the OCI blob store (filename there was already this digest) | `55afa1ecc21d2bb5e5045f32dafee56272ffd89860bac26f6c32123439af26a4` |
| `alpine-arm64.tar.gz` | `alpine:latest`, `linux/arm64`, same method | `5de55e5ef9c033997441461efe7ba23a986db059c0bb78b38f84ee0d72b99167` |
| `busybox-amd64.tar.gz` | `busybox:latest`, `linux/amd64`, same method | `b05093807bb0294152bb9cf86d64da722732dddaf7f8882fa1f120477dbc4db3` |
| `busybox-arm64.tar.gz` | `busybox:latest`, `linux/arm64`, same method | `025fe1949698376d1d9a946f8a39a3529ad3ea540ca92b78c6cd041deb19d63e` |
| `zipbomb-lookalike.tar.gz` | Synthesized locally, not derived from the real 42.zip (which fully unpacks to ~4.5PB and is unsafe to touch even partially). Three files (~20MB all-zero, ~20MB repeating 1KB pattern, ~9.4MB mixed sparse-zero/random blocks) tarred and gzipped, inspired by 42.zip's pathological-compressibility shape at a safe, bounded size | `c4a58bf4bc5bbe9c739a881092b80d80b99aa675b18e02f436165453866b3c44` |
| `linux-0.01.tar.gz` | Linus Torvalds' original 1991 Linux 0.01 kernel source release, fetched from `https://mirrors.edge.kernel.org/pub/linux/kernel/Historic/linux-0.01.tar.gz` — checksum matches kernel.org's own published `sha256sums.asc` for this file | `24454f830cdb571e2c4ad15481119c43b3cafd48dd869a9b2945d1036d1dc68d` |
| `doomsrc.tgz` | id Software's official 1997 Doom source release. Fetched `idstuff/source/doomsrc.zip` (id 8802 in the idgames archive, mirrored at `https://youfailit.net/pub/idgames/`) and extracted its inner `linuxdoom-1.10.src.tgz` unmodified — that inner file is itself the authentic tgz id Software originally packaged | `e178853e9878b205bba177191a871aae76be3c69c83b4e127de63275070b9e6b` |

## Known limitations

- **Seven files is still a small dataset**, same limitation screencam notes
  for its five photos. The off-limits-`prepare.go` rule stops literal
  cheating, but nothing stops `train.go` from branching on a file's exact
  size or byte signature in a technically-compliant but overfit way.
  Growing the dataset is a human decision, not something a round should do
  itself.
- **The compute budget is relative, not absolute.** A pathological
  `train.go` that's simply slow in a way proportional to input size (rather
  than genuinely hung) could still be tediously slow in absolute terms on
  the larger files while staying under its own 100x cap. This hasn't come
  up in practice but is a known gap, not an oversight.

## How this compares to the reference projects

Structurally identical to both: `prepare`/`train` split with the grader
read-only, `program.md` as the fresh-read-every-round briefing, a single
scalar keep-if-improved metric, git-commit-tracked history, human-triggered
per round rather than an unattended loop.

Deliberately different: the correctness bar is exact byte-identical
round-tripping rather than a fuzzy OCR/geometry score, there's no
constants-allowlist negotiation protocol (compression tuning legitimately
needs too many numeric parameters for a blanket literal-ban to make sense),
and the hang-protection mechanism is a per-file compute-budget multiple
instead of one fixed wall-clock cutoff, since this dataset's file sizes
span two orders of magnitude.
