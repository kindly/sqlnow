# AUR packaging

Two packages, both repackaging the GitHub release artifacts:

- `sqlnow-bin` — the command-line server, from the linux `.tar.gz`.
- `sqlnow-desktop-bin` — the window version, unpacked from the AppImage so it
  needs no FUSE at runtime.

(A from-source PKGBUILD existed briefly — see git history — but the bundled
DuckDB compile makes source installs slow for no benefit, so it was dropped.)

The copies in this directory are **for the record, not the source of truth**:
edits happen in the AUR clones, and these are refreshed from them at release
time. The `sqlnow-bin` copy here once sat seven versions behind and had missed
an `options=('!strip' '!debug')` fix entirely, which is why the direction is
written down this way now.

## Per release

The clones live outside this repo (gitignored at the root as `sqlnow-bin/` and
`sqlnow-desktop-bin/`), one per package. In each:

```bash
# 1. bump pkgver, reset pkgrel=1
# 2. refresh the checksums against the new release files
updpkgsums                          # or sha256sum the downloads yourself
# 3. verify it builds, and look inside the result
makepkg -f --clean
tar --zstd -tvf *.pkg.tar.zst
# 4. regenerate the metadata the AUR actually reads
makepkg --printsrcinfo > .SRCINFO
# 5. publish
git add PKGBUILD .SRCINFO
git commit -m "0.4.7"
git push origin HEAD:master         # AUR only accepts the master branch!
```

Then copy both PKGBUILDs back into this directory so the repo records what
shipped.

## Worth checking in the built package

Cheap, and each has been wrong at least once:

- `sqlnow --version` from the packaged binary matches the tag.
- `--agents-help` output equals the repo's `AGENTS.md` — it is compiled in, so
  a stale build ships stale instructions.
- **desktop only:** `chrome-sandbox` is `-rwsr-xr-x`. Without the setuid bit
  Chromium's fallback sandbox cannot start where unprivileged user namespaces
  are disabled.
- **desktop only:** `usr/lib/sqlnow-desktop-bin/` is `drwxr-xr-x`.
  `--appimage-extract` unpacks 0700 directories and `cp -a` copies that
  faithfully, so installed as root nothing else could traverse them.

## Gotchas learned the hard way

- **AUR rejects any branch except `master`.** With `init.defaultBranch=main`,
  either rename (`git branch -m main master`) or push `HEAD:master`.
- **`.SRCINFO` must be regenerated on every change** — the AUR site reads it,
  not the PKGBUILD.
- `updpkgsums` comes from `pacman-contrib`, which is not always installed;
  `sha256sum` on the downloaded files does the same job.
