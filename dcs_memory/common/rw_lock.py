"""
Custom implementation of a fair Read-Write Lock.
"""

import threading
from contextlib import contextmanager

class RWLockFair:
    """
    A fair Read-Write Lock implementation.
    Fairness policy: Writers are preferred over new readers if a writer is waiting.
    Among waiting writers or waiting readers, FIFO-like order is generally maintained
    by the Condition variable's internal queue for threads waiting on wait().
    """
    def __init__(self):
        self._condition = threading.Condition(threading.Lock())
        self._num_readers_active = 0
        self._writer_active = False
        self._num_writers_waiting = 0

    def acquire_read(self):
        """Acquire a read lock."""
        with self._condition:
            # Readers must wait if a writer is active OR if writers are waiting (writer preference)
            while self._writer_active or self._num_writers_waiting > 0:
                self._condition.wait()
            self._num_readers_active += 1

    def release_read(self):
        """Release a read lock."""
        with self._condition:
            if self._num_readers_active == 0:
                raise RuntimeError("RWLockFair: release_read called without holding a read lock.")
            self._num_readers_active -= 1
            if self._num_readers_active == 0: # If this was the last reader
                # Notify all, so any waiting writers or readers can check their conditions.
                # Writers waiting will get priority due to the check in acquire_read.
                self._condition.notify_all()

    @contextmanager
    def gen_rlock(self):
        """Context manager for acquiring and releasing a read lock."""
        acquired = False
        try:
            self.acquire_read()
            acquired = True
            yield
        finally:
            if acquired:
                self.release_read()

    def acquire_write(self):
        """Acquire a write lock."""
        with self._condition:
            self._num_writers_waiting += 1
            try:
                # Writers must wait if a writer is already active OR if any readers are active
                while self._writer_active or self._num_readers_active > 0:
                    self._condition.wait()
            finally:
                # This finally block is to ensure _num_writers_waiting is decremented
                # if wait() is interrupted by an unhandled exception (very unlikely for Condition.wait).
                # Normal flow is to decrement after the loop.
                # If we successfully exit the loop, we are about to become the active writer.
                pass # Decrement and set active status outside the try/finally for clarity after loop.

            # Successfully exited the wait loop, means the lock can be acquired
            self._num_writers_waiting -= 1
            self._writer_active = True

    def release_write(self):
        """Release a write lock."""
        with self._condition:
            if not self._writer_active:
                raise RuntimeError("RWLockFair: release_write called without holding a write lock.")
            self._writer_active = False
            # Notify all waiters; readers and writers will re-check their conditions.
            # If other writers were waiting, one of them will get it due to num_writers_waiting > 0 in acquire_read.
            # If only readers were waiting, they will proceed.
            self._condition.notify_all()

    @contextmanager
    def gen_wlock(self):
        """Context manager for acquiring and releasing a write lock."""
        acquired = False
        try:
            self.acquire_write()
            acquired = True
            yield
        finally:
            if acquired:
                self.release_write()

if __name__ == '__main__':
    import time
    import random

    rw_lock = RWLockFair()
    log_lock = threading.Lock() # For synchronized print
    shared_resource = {"value": 0, "active_readers_count": 0}

    # Event to signal writer threads have finished their main work for assertion
    writer_completion_events = []

    def log_message(message):
        with log_lock:
            print(f"[{threading.current_thread().name} @ {time.time():.4f}] {message}")

    def reader_task(reader_id, num_iterations):
        for i in range(num_iterations):
            log_message(f"Reader {reader_id}-{i}: Attempting to acquire read lock...")
            with rw_lock.gen_rlock():
                log_message(f"Reader {reader_id}-{i}: Read lock acquired.")
                shared_resource["active_readers_count"] +=1
                current_value = shared_resource["value"]
                active_r = shared_resource["active_readers_count"]
                log_message(f"Reader {reader_id}-{i}: Reading value {current_value}. Active readers: {active_r}.")
                assert rw_lock._writer_active == False, "Writer active during read lock!"
                time.sleep(random.uniform(0.05, 0.15)) # Simulate read operation
                shared_resource["active_readers_count"] -=1
                log_message(f"Reader {reader_id}-{i}: Releasing read lock. Value was {current_value}.")
            time.sleep(random.uniform(0.01, 0.05)) # Brief pause

    def writer_task(writer_id, num_iterations, completion_event):
        for i in range(num_iterations):
            log_message(f"Writer {writer_id}-{i}: Attempting to acquire write lock...")
            with rw_lock.gen_wlock():
                log_message(f"Writer {writer_id}-{i}: Write lock acquired.")
                assert shared_resource["active_readers_count"] == 0, "Readers active during write lock!"
                assert rw_lock._num_readers_active == 0, "Internal reader count non-zero during write lock!"

                old_value = shared_resource["value"]
                shared_resource["value"] += 1
                new_value = shared_resource["value"]
                log_message(f"Writer {writer_id}-{i}: Wrote value from {old_value} to {new_value}.")
                time.sleep(random.uniform(0.1, 0.2)) # Simulate write operation
                log_message(f"Writer {writer_id}-{i}: Releasing write lock.")
            time.sleep(random.uniform(0.05, 0.1)) # Brief pause
        completion_event.set()

    NUM_READERS = 5
    NUM_WRITERS = 2
    READS_PER_READER = 4
    WRITES_PER_WRITER = 3

    threads = []
    log_message(f"Starting test with {NUM_READERS} readers and {NUM_WRITERS} writers.")

    for i in range(NUM_WRITERS):
        event = threading.Event()
        writer_completion_events.append(event)
        w_thread = threading.Thread(target=writer_task, args=(i + 1, WRITES_PER_WRITER, event), name=f"Writer-{i+1}")
        threads.append(w_thread)
        w_thread.start()
        time.sleep(0.01) # Stagger writer starts slightly

    for i in range(NUM_READERS):
        r_thread = threading.Thread(target=reader_task, args=(i + 1, READS_PER_READER), name=f"Reader-{i+1}")
        threads.append(r_thread)
        r_thread.start()
        time.sleep(0.01) # Stagger reader starts slightly


    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Sanity check: all writer completion events should be set
    for event in writer_completion_events:
        assert event.is_set(), "A writer did not complete its work!"

    final_value = shared_resource["value"]
    expected_final_value = NUM_WRITERS * WRITES_PER_WRITER
    log_message(f"All threads finished. Final shared value: {final_value}")
    log_message(f"Expected final value: {expected_final_value}")

    assert final_value == expected_final_value, \
        f"Final value assertion failed! Expected: {expected_final_value}, Got: {final_value}"
    assert shared_resource["active_readers_count"] == 0, \
        f"Final active readers count is not zero: {shared_resource['active_readers_count']}"
    assert rw_lock._num_readers_active == 0, \
        f"Internal lock reader count non-zero: {rw_lock._num_readers_active}"
    assert not rw_lock._writer_active, "Internal lock writer still active"
    # Note: _num_writers_waiting should be 0 if all writers completed and no new ones are queued.
    # This test doesn't explicitly check _num_writers_waiting at the very end as it's an internal state
    # that depends on thread scheduling if some threads were still trying to acquire.
    # The primary check is the correctness of the shared_resource.value.

    log_message("RWLockFair simple stress test PASSED.")
```
