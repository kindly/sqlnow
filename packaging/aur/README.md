# AUR packaging

Two packages, each its own git repo on the AUR:

- `sqlnow-bin` — repackages the GitHub release binaries (primary package)
- `sqlnow` — builds from the release source tarball (compiles bundled DuckDB;
  long build)

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

Same for `sqlnow` (ssh://aur@aur.archlinux.org/sqlnow.git).

Per release:

1. Bump `pkgver`, reset `pkgrel=1` in both PKGBUILDs here.
2. Refresh `sha256sums*` against the new release artifacts (`updpkgsums`,
   or sha256sum the downloaded tarballs).
3. Copy to the AUR clones, rebuild, regenerate `.SRCINFO`, commit, push.

## Gotchas learned the hard way

- **AUR rejects any branch except `master`.** With `init.defaultBranch=main`,
  either rename (`git branch -m main master`) or push `HEAD:master`.
- **`.SRCINFO` must be regenerated on every change** — the AUR site reads it,
  not the PKGBUILD.
- **`options=('!lto')` is required in the source package**: makepkg's default
  LTO flags make GCC emit fat-LTO objects for the bundled DuckDB C++, which
  rust-lld cannot link.
- The GitHub source tarball extracts to `sqlnow-$pkgver` (repo was renamed
  from `querier`).
