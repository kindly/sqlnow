fn main() {
    hide_sqlite_symbols();
    tauri_build::build()
}

/// Keep this binary's sqlite to itself.
///
/// duckdb's sqlite scanner extension carries its own complete sqlite — 297
/// symbols, none of them undefined. This binary contains a second one, linked
/// in statically through rusqlite, and linking webkit (which pulls in the
/// system libsqlite3) makes the linker export ours into the dynamic symbol
/// table. The extension is dlopened, so the global scope is searched first and
/// its internal calls bind to *our* sqlite rather than its own: two different
/// builds sharing one set of structs, which segfaults on the first attach of a
/// sqlite database.
///
/// Hiding just `sqlite3_*` fixes it. Everything else stays exported, which
/// matters because those extensions call back into duckdb here. The plain CLI
/// exports no sqlite symbols at all, which is why it never had the problem.
fn hide_sqlite_symbols() {
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("linux") {
        return;
    }
    let out_dir = std::env::var("OUT_DIR").expect("cargo sets OUT_DIR");
    let script = std::path::Path::new(&out_dir).join("hide-sqlite.map");
    std::fs::write(&script, "{ global: *; local: sqlite3_*; };\n")
        .expect("writing the version script");
    println!("cargo:rustc-link-arg=-Wl,--version-script={}", script.display());
    println!("cargo:rerun-if-changed=build.rs");
}
