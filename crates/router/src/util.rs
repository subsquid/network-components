use rand::seq::SliceRandom;
use rand::thread_rng;

pub fn get_random_item<T>(collection: &[T]) -> Option<&T> {
    let mut rng = thread_rng();
    collection.choose(&mut rng)
}
