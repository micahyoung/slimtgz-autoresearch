# slimtgz-autoresearch

You are one round in an ongoing, fully stateless self-improvement loop. You
have no memory of any prior round — this file and `git log -- .` (run from
the repo root as `git log -- slimtgz-autoresearch`, or just `git log` if
this is the repo root) are the *only* continuity you have. Read both before
doing anything. (`README.md` has more background if you want it, but
nothing in it is required for the work below.)

## The task

`data/` holds six real-world `.tar.gz` files: OCI container filesystem
layers (`alpine-amd64`, `alpine-arm64`, `busybox-amd64`, `busybox-arm64`),
the original 1991 Linux 0.01 kernel source (`linux001`), and id Software's
original 1997 Doom source release (`doomsrc`). The goal is a CLI, `train.go`,
that reads a `.tar.gz` file and writes a smaller-or-equal-size `.tar.gz` copy
that re-extracts to exactly the same content.

**The hard invariant**: for every file name present in the input, gunzip+
tar-extracting the output must produce an entry under that same name with
every header field (mode, ownership, link target, timestamps, everything)
and every byte of content matching exactly. Where a name appears more than
once, whichever entry comes last wins — the normal tar-extraction rule.
There is no allowance for "close enough," and no allowance for dropping
something on the theory that the grader "doesn't look at it" — if it's
part of the archive, it has to survive. Any deviation is a disqualifying
failure, not a bad score. Within that constraint, anything is fair game —
the only fixed points are the output being valid gzip and the
re-extraction being exact.

## The two files, and the hard rule

- **`prepare.go` — you may read it, you may never edit it.** It holds the
  dataset, the round-trip verifier, and the compute-budget enforcement.
  This is the grader. If you could edit your own grader you could always
  "pass" — that would make every round pointless. It also refuses to run at
  all if it, `program.md`, or `README.md` have uncommitted changes — if you
  see that error, commit or discard first (never edit `README.md`/
  `program.md` yourself — those aren't your files to change).
- **`train.go` — this is the only file you ever edit.** Its comments may
  only explain what the code currently does — never a per-round worklog of
  attempts and outcomes; that belongs in commit messages (step 6) and
  nowhere else.

Run a round with:

```
go run prepare.go
```

This builds your current `train.go`, runs it against all six dataset
files (each as `train <in> <out>`), verifies the round-trip invariant, and
prints a per-file and aggregate result.

Go deps live in the human-owned `go.mod`/`go.sum` — don't edit those files.
A dependency you need but that's missing is reported to the human, not
added here.

## Workflow for a round

1. Read this file and `git log -- .` to see what's already been tried and
   what the current round number is (count prior commits scoped to this
   directory, add 1).
2. Decide on ONE focused change to `train.go`. Small, reviewable diffs —
   not a rewrite.
3. Run `go run prepare.go`. Read the full output: per-file `ratio` and
   `compute`, and the `AGGREGATE LOSS` / `COMPUTE RATIO` lines.
4. **Two numbers matter, and they're kept separate on purpose**:
   - `AGGREGATE LOSS` — `max()` of all six files' `output_size /
     input_size` (worst-of-6, not an average: a great ratio on some files
     must never mask a failure on another), lower is better.
   - `COMPUTE RATIO` — `max()` across the six files of how many times
     longer `train.go` took than an in-process `gzip.BestCompression` pass
     over the same content took, lower is simpler/cheaper. Each file gets
     its own budget of 100x its own baseline, so this cap scales with input
     size instead of being one fixed wall-clock number across files ranging
     from 73KB to 4MB. **Exceeding the cap on any file is not a bad score —
     it's a disqualification.** `prepare.go` kills the subprocess and the
     round produces no scorable result at all, the same severity as a
     round-trip mismatch or a crash.
5. **Keep-or-discard policy** (round-trip must pass and compute must stay
   within budget on every file, regardless):
   - Any file disqualified (round-trip failure or over the compute budget):
     revert (`git checkout -- train.go`) and try something else. There is
     nothing to keep or discard here — it isn't a scored result.
   - Loss improved (went down): keep.
   - Loss got *worse*: discard, regardless of compute ratio.
   - Loss roughly flat (within 0.01) but compute ratio dropped: keep — a
     real efficiency win.
   - Loss roughly flat (within 0.01) and compute ratio rose (but still
     within budget): discard — spent more compute for no measurable gain.
6. If you kept the change: commit it — **only paths under this
   directory** — with a message that carries the full round record, since
   there is no separate history file; `git log` on this directory *is* the
   history:

   ```
   git add slimtgz-autoresearch
   git commit -m "slimtgz-autoresearch round N: loss=X.XXXX (alpine-amd64=A alpine-arm64=B busybox-amd64=C busybox-arm64=D linux001=E doomsrc=F)

   one-line description of what changed and why"
   ```

   Never touch anything outside `slimtgz-autoresearch/` in this repo. (If
   this directory *is* the repo root, the `git add`/commit paths are just
   the files themselves.)

## Crashes and disqualifications

If `go run prepare.go` reports a build failure, a crash, a round-trip
mismatch, or a disqualification (over the compute budget on any file):

- Something dumb and easy to fix (a typo, a nil check, an off-by-one)? Fix
  it and re-run.
- The idea itself is fundamentally broken? Revert
  (`git checkout -- train.go`), try a different idea. There's no results
  file to log a failed attempt into — git only ever sees the commits you
  actually keep, so an abandoned attempt just leaves no trace, which is
  fine.
- There is no separate fixed-timeout rule to worry about here: the 100x
  per-file compute budget in step 4 *is* the hang protection, and it scales
  with each file automatically. If a file blows through its own budget,
  that's `prepare.go` telling you the change needs to be reverted, not a
  signal to raise a limit yourself — you can't edit `prepare.go` to do that
  anyway.
