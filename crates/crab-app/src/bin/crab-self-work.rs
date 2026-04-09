#[cfg(not(test))]
fn main() {
    let (mut out, mut err) = (std::io::stdout(), std::io::stderr());
    std::process::exit(crab_app::run_self_work_cli(
        std::env::args(),
        &mut out,
        &mut err,
    ));
}
