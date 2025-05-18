// Copyright (c) Microsoft Corporation.

/// Provides concurrency utilities.
///
/// This module defines a `SharedState` struct that can be used to manage shared state signal in a concurrent context.
use std::sync::{Condvar, Mutex};

/// A semaphore-like structure for managing shared state across multiple threads while supporting signaling and awaiting conditions across threads.
///
/// This structure provides a way to manage shared state across multiple threads. It includes a condition variable
/// for notifying threads when they should stop, a counter for tracking the number of active threads, and a second
/// condition variable for notifying threads when all child threads have finished.
pub struct SharedState {
    /// A condition variable used to notify all threads when they should stop.
    cnd: Condvar,
    /// A mutex-protected boolean that indicates whether threads should stop.
    should_stop: Mutex<bool>,
    /// A condition variable used to notify all threads when all child threads have finished.
    all_children_done: Condvar,
    /// A mutex-protected counter that tracks the number of active child threads.
    child_ctr: Mutex<i32>,
}

impl SharedState {
    /// Creates a new `SharedState`.
    ///
    /// The `SharedState` starts with `should_stop` set to `false` and `child_ctr` set to `0`.
    pub fn new() -> Self {
        SharedState {
            cnd: Condvar::new(),
            should_stop: Mutex::new(false),
            all_children_done: Condvar::new(),
            child_ctr: Mutex::new(0),
        }
    }

    /// Signals all threads to stop.
    ///
    /// This function sets `should_stop` to `true` and notifies all threads waiting on `cnd`.
    pub fn stop(&self) {
        let mut should_stop = self.should_stop.lock().unwrap();
        *should_stop = true;
        self.cnd.notify_all();
    }

    /// Waits for the stop signal condition to be set to true, blocks the thread until then.
    pub fn wait_for_stop_signal(&self) {
        let mut should_stop = self.should_stop.lock().unwrap();
        while !*should_stop {
            should_stop = self.cnd.wait(should_stop).unwrap();
        }
    }

    /// Increments the child counter.
    ///
    /// This function is typically called when a new child thread is started.
    pub fn incr(&self) {
        let mut child_ctr = self.child_ctr.lock().unwrap();
        *child_ctr += 1;
    }

    /// Decrements the child counter and notifies all threads if all child threads have finished.
    ///
    /// This function is typically called when a child thread finishes. If the child counter reaches `0`,
    /// this function notifies all threads waiting on `all_children_done`.
    pub fn decr_and_notify_all_children_done_awaiters(&self) {
        let mut child_ctr = self.child_ctr.lock().unwrap();
        *child_ctr -= 1;
        if *child_ctr == 0 {
            self.all_children_done.notify_all();
        }
    }

    /// Waits for all child threads to finish, blocks the thread until then.
    pub fn wait_all_children_done(&self) {
        let mut num_children = self.child_ctr.lock().unwrap();
        while *num_children > 0 {
            num_children = self.all_children_done.wait(num_children).unwrap();
        }
    }
}
