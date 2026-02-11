#[cfg(not(test))]
fn main() {
    let (mut out, mut err) = (std::io::stdout(), std::io::stderr());
    let args: Vec<String> = std::env::args().collect();
    std::process::exit(crab_app::run_installer_cli(&args, &mut out, &mut err));
}
