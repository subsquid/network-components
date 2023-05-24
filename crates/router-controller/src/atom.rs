//
// Attempt to develop analog of
// https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/atomic/AtomicReference.html
//
use parking_lot::RwLock;
use std::fmt::{Debug, Formatter};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

pub struct Atom<T: ?Sized> {
    inner: RwLock<Arc<T>>,
}

unsafe impl<T: ?Sized + Send> Send for Atom<T> {}
unsafe impl<T: ?Sized + Send> Sync for Atom<T> {}

impl<T: ?Sized> Atom<T> {
    pub fn new(val: Arc<T>) -> Self {
        Atom {
            inner: RwLock::new(val),
        }
    }

    pub fn get(&self) -> Arc<T> {
        self.inner.read().deref().clone()
    }

    pub fn set(&self, val: Arc<T>) {
        let mut lock = self.inner.write();
        *lock.deref_mut() = val
    }

    pub fn update<F: FnMut(&T) -> Option<Arc<T>>>(&self, mut f: F) {
        loop {
            let initial = self.get();
            if let Some(new_val) = f(initial.deref()) {
                let mut lock = self.inner.write();
                let current = lock.deref_mut();
                if Arc::ptr_eq(&initial, current) {
                    *current = new_val;
                    return;
                }
            } else {
                return;
            }
        }
    }
}

impl<T: ?Sized> Clone for Atom<T> {
    fn clone(&self) -> Self {
        Atom::new(self.get())
    }
}

impl<T: Debug> Debug for Atom<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.get().fmt(f)
    }
}
