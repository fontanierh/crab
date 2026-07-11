pub fn manual_copy(source: &[i32], destination: &mut [i32]) {
    for index in 0..source.len() {
        destination[index] = source[index];
    }
}
