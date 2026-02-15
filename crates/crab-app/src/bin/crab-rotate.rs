#[cfg(not(test))]
fn main() {
    let (mut stdin, mut out, mut err) = (std::io::stdin(), std::io::stdout(), std::io::stderr());
    std::process::exit(crab_app::run_rotate_cli(
        std::env::args(),
        &mut stdin,
        &mut out,
        &mut err,
    ));
}
