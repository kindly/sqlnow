# AUR packaging

One package: `sqlnow-bin`, repackaging the GitHub release binaries.
(A from-source PKGBUILD existed briefly — see git history — but the bundled
DuckDB compile makes source installs slow for no benefit, so it was dropped.)

## Publishing / updating

```bash
git clone ssh://aur@aur.archlinux.org/sqlnow-bin.git   # empty on first clone
cd sqlnow-bin
cp path/to/repo/packaging/aur/sqlnow-bin/PKGBUILD .
makepkg -f --clean                 # verify it builds
makepkg --printsrcinfo > .SRCINFO  # required; AUR reads metadata from this
git add PKGBUILD .SRCINFO
git commit -m "0.3.0"
git push origin HEAD:master        # AUR only accepts the master branch!
```

Per release:

1. Bump `pkgver`, reset `pkgrel=1` in the PKGBUILD here.
2. Refresh `sha256sums_*` against the new release tarballs (`updpkgsums`,
   or sha256sum the downloaded files).
3. Copy to the AUR clone, rebuild, regenerate `.SRCINFO`, commit, push.

## Gotchas learned the hard way

- **AUR rejects any branch except `master`.** With `init.defaultBranch=main`,
  either rename (`git branch -m main master`) or push `HEAD:master`.
- **`.SRCINFO` must be regenerated on every change** — the AUR site reads it,
  not the PKGBUILD.
