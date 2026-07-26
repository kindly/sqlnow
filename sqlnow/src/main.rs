use eyre::Result;
use sqlnow::{parse_args, prepare, query_url, run_immediate, serve};

#[actix_web::main]
async fn main() -> Result<()> {
    let (cli, matches) = parse_args()?;

    if run_immediate(&cli)? {
        return Ok(());
    }

    let prepared = prepare(&cli, &matches).await?;

    // bind before announcing anything: a failed bind (port already in use)
    // must not print "Server running" or open a browser tab at a dead URL
    let host = prepared.host.clone();
    let (server, addr) = serve(prepared.app_data, &host, prepared.port.unwrap_or(8080))?;

    let base_url = format!("http://{}:{}", host, addr.port());
    // the port is only known now, so this is where the session becomes findable
    prepared.closer.mark_live(&base_url);
    println!("Server running on {}", base_url);

    let deep_url = query_url(&base_url, prepared.open_query.as_deref());
    if let (Some(name), Some(url)) = (&prepared.open_query, &deep_url) {
        println!("Open query \"{}\": {}", name, url);
    }

    if cli.open.is_some() {
        let target = deep_url.clone().unwrap_or_else(|| base_url.clone());
        if let Err(e) = open::that_detached(&target) {
            eprintln!("Could not open the browser: {}", e);
        }
    }

    server.await?;

    // the session was last used just now, not when it opened
    prepared.closer.mark_used();

    Ok(())
}
