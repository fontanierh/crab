pub fn almost_swap(mut left: i32, mut right: i32) -> (i32, i32) {
    left = right;
    right = left;
    (left, right)
}
