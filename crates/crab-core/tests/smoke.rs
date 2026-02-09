use crab_core::crate_id;

#[test]
fn public_api_is_stable() {
    assert_eq!(crate_id(), "crab-core");
}
