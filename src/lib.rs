//! Hybrid locking, or [`parking_lot::RwLock`] with support for optimistic locking.
//!
//! See [the paper](https://dl.acm.org/doi/abs/10.1145/3399666.3399908) for details.

use std::{
    ops::{Deref, DerefMut},
    sync::atomic::{fence, AtomicU64, Ordering},
};

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

/// RAII structure used to release the shared read access of a lock when dropped.
pub struct HybridRwLockReadGuard<'a, T> {
    guard: RwLockReadGuard<'a, T>,
    rw_lock: &'a HybridLock<T>,
}

impl<'a, T> Deref for HybridRwLockReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

/// RAII structure used to release the exclusive write access of a lock when dropped.
pub struct HybridRwLockWriteGuard<'a, T> {
    guard: RwLockWriteGuard<'a, T>,
    rw_lock: &'a HybridLock<T>,
}

impl<'a, T> Deref for HybridRwLockWriteGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

impl<'a, T> DerefMut for HybridRwLockWriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard.deref_mut()
    }
}

/// A hybrid lock.
pub struct HybridLock<T> {
    // T will be in `UnsafeCell`.
    rw_lock: RwLock<T>,
    version: AtomicU64,
}

impl<T> HybridLock<T> {
    /// Creates a new instance of [`HybridLock`].
    pub fn new(t: T) -> HybridLock<T> {
        HybridLock {
            rw_lock: RwLock::new(t),
            version: AtomicU64::default(),
        }
    }

    /// Locks this hybrid lock with shared read access.
    ///
    /// The calling thread will be blocked until there is no writer which holds the lock.
    pub fn read(&self) -> HybridRwLockReadGuard<T> {
        let guard = self.rw_lock.read();
        HybridRwLockReadGuard {
            guard,
            rw_lock: self,
        }
    }

    /// Locks this hybrid lock with exclusive write access.
    ///
    /// The calling thread will be blocked until there are no readers or writers which hold the lock.
    pub fn write(&self) -> HybridRwLockWriteGuard<T> {
        let guard = self.rw_lock.write();
        HybridRwLockWriteGuard {
            guard,
            rw_lock: self,
        }
    }

    /// Runs the given callback without acquiring the lock with fallback mode.
    ///
    /// The calling thread will be blocked when falling back to acquiring a shared access.
    /// This will happen when the optimisic run fails due to a concurrent writer.
    #[doc = include_str!("./callback-safety.md")]
    pub unsafe fn optimistic<F, R>(&self, f: F) -> R
    where
        F: Fn(*const T) -> R,
    {
        if let Some(result) = self.try_optimistic(&f) {
            result
        } else {
            self.fallback(f)
        }
    }

    /// Runs the given callback without acquiring the lock.
    #[doc = include_str!("./callback-safety.md")]
    pub unsafe fn try_optimistic<F, R>(&self, f: F) -> Option<R>
    where
        F: Fn(*const T) -> R,
    {
        if self.rw_lock.is_locked_exclusive() {
            return None;
        }

        let pre_version = self.current_version();
        let result = f(self.rw_lock.data_ptr());

        if self.rw_lock.is_locked_exclusive() {
            return None;
        }

        let post_version = self.current_version();
        if pre_version == post_version {
            Some(result)
        } else {
            None
        }
    }

    /// Returns a raw pointer to the underlying data.
    ///
    /// This is useful when you want to validate the optimisitc operations by yourself,
    /// e.g., when the operation involves one or more optimistic locks.
    ///
    /// ## Example
    ///
    /// ```rust
    /// # use hybrid_lock::HybridLock;
    /// let a = HybridLock::new(1);
    /// let pre_version = a.current_version();
    /// let val = unsafe { a.data_ptr().read() };
    /// let post_version = a.current_version();
    /// if pre_version == post_version {
    ///     // No concurrent writer.
    ///     assert_eq!(val, 1);
    /// }
    /// ```
    pub fn data_ptr(&self) -> *const T {
        self.rw_lock.data_ptr() as *const T
    }

    /// Gets the current version of this lock.
    ///
    /// ## Example
    ///
    /// ```rust
    /// # use hybrid_lock::HybridLock;
    /// let a = HybridLock::new(1);
    /// let pre_version = a.current_version();
    /// let val = unsafe { a.data_ptr().read() };
    /// let post_version = a.current_version();
    /// if pre_version == post_version {
    ///     // No concurrent writer.
    ///     assert_eq!(val, 1);
    /// }
    /// ```
    pub fn current_version(&self) -> u64 {
        // This `atomic::fence` prevents the reordering of `is_locked_exclusive()` and `self.version.load`.
        // This is necessary as we don't know whether the RwLock uses the memory ordering strong enough to
        // prevent such reordering.
        fence(Ordering::Acquire);
        self.version.load(Ordering::Acquire)
    }

    fn fallback<F, R>(&self, f: F) -> R
    where
        F: Fn(*const T) -> R,
    {
        let guard = self.read();
        f(guard.rw_lock.rw_lock.data_ptr() as *const T)
    }
}

impl<'a, T> Drop for HybridRwLockWriteGuard<'a, T> {
    fn drop(&mut self) {
        self.rw_lock.version.fetch_add(1, Ordering::Release);
    }
}
