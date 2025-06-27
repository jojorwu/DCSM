# Common utility functions and classes
import asyncio
import logging
from threading import RLock

logger = logging.getLogger(__name__)

class RWLock:
    """
    A Read-Write Lock implementation that allows multiple readers or a single writer.
    This version is designed to be fair to writers, preventing writer starvation.
    It uses asyncio for synchronization primitives.
    """
    def __init__(self):
        self._read_condition = asyncio.Condition()
        self._write_condition = asyncio.Condition()
        self._readers_count = 0
        self._writer_active = False
        self._writers_waiting = 0 # Count of writers waiting to acquire the lock

    async def acquire_read(self):
        """Acquire a read lock."""
        async with self._read_condition:
            # If there's an active writer or writers are waiting, wait.
            # This gives priority to waiting writers to prevent starvation.
            while self._writer_active or self._writers_waiting > 0:
                await self._read_condition.wait()
            self._readers_count += 1

    async def release_read(self):
        """Release a read lock."""
        async with self._read_condition:
            if self._readers_count == 0:
                # This should ideally not happen if acquire/release are used correctly.
                logger.warning("RWLock: release_read called when no readers were active.")
                return

            self._readers_count -= 1
            if self._readers_count == 0:
                # If there are no more readers, notify any waiting writers.
                # We only need to notify one writer if multiple are waiting.
                async with self._write_condition:
                    self._write_condition.notify(n=1) # Notify one waiting writer

    async def acquire_write(self):
        """Acquire a write lock."""
        async with self._write_condition:
            self._writers_waiting += 1
            while self._writer_active or self._readers_count > 0:
                await self._write_condition.wait()
            self._writers_waiting -= 1
            self._writer_active = True

    async def release_write(self):
        """Release a write lock."""
        async with self._write_condition: # Use write_condition for mutual exclusion on writer state
            if not self._writer_active:
                 # This should ideally not happen.
                logger.warning("RWLock: release_write called when no writer was active.")
                return

            self._writer_active = False
            # Notify all waiting readers and one waiting writer.
            # Readers can proceed in parallel, but only one writer should proceed.
            # Prioritize notifying a writer first if any are waiting.
            if self._writers_waiting > 0:
                self._write_condition.notify(n=1)
            else:
                # If no writers are waiting, notify all readers.
                async with self._read_condition:
                    self._read_condition.notify_all()

    # Context managers for easy usage
    class _ReadLockContext:
        def __init__(self, lock):
            self._lock = lock
        async def __aenter__(self):
            await self._lock.acquire_read()
            return self
        async def __aexit__(self, exc_type, exc_val, exc_tb):
            await self._lock.release_read()

    class _WriteLockContext:
        def __init__(self, lock):
            self._lock = lock
        async def __aenter__(self):
            await self._lock.acquire_write()
            return self
        async def __aexit__(self, exc_type, exc_val, exc_tb):
            await self._lock.release_write()

    def read_lock(self):
        """Returns an async context manager for read lock."""
        return self._ReadLockContext(self)

    def write_lock(self):
        """Returns an async context manager for write lock."""
        return self._WriteLockContext(self)

class ThreadSafeCounter:
    """
    A simple thread-safe counter.
    Uses a reentrant lock to allow the same thread to acquire the lock multiple times.
    """
    def __init__(self, initial_value: int = 0):
        self._value = initial_value
        self._lock = RLock() # threading.RLock

    def increment(self, amount: int = 1) -> int:
        """Increments the counter by a given amount and returns the new value."""
        with self._lock:
            self._value += amount
            return self._value

    def decrement(self, amount: int = 1) -> int:
        """Decrements the counter by a given amount and returns the new value."""
        with self._lock:
            self._value -= amount
            return self._value

    def get_value(self) -> int:
        """Returns the current value of the counter."""
        with self._lock:
            return self._value

    def set_value(self, value: int) -> None:
        """Sets the counter to a specific value."""
        with self._lock:
            self._value = value

def generate_kem_id(kem_type: str, entity_id: str, attribute_name: str) -> str:
    """
    Generates a unique KEM ID.
    Example: "user_profile:12345:email_address"
    """
    if not all([kem_type, entity_id, attribute_name]):
        raise ValueError("kem_type, entity_id, and attribute_name must all be non-empty.")
    return f"{kem_type}:{entity_id}:{attribute_name}"

def parse_kem_id(kem_id: str) -> tuple[str, str, str]:
    """
    Parses a KEM ID into its components.
    Returns a tuple: (kem_type, entity_id, attribute_name)
    Raises ValueError if the kem_id format is invalid.
    """
    parts = kem_id.split(':', 2)
    if len(parts) != 3:
        raise ValueError(f"Invalid KEM ID format: {kem_id}. Expected 'type:id:attribute' with at least 3 parts.")

    kem_type, entity_id, attribute_name = parts[0], parts[1], parts[2]

    if not all([kem_type, entity_id, attribute_name]):
        # This aligns with generate_kem_id's requirement for all parts to be non-empty.
        raise ValueError(f"Invalid KEM ID: '{kem_id}'. All parts (type, id, attribute) must be non-empty.")

    return kem_type, entity_id, attribute_name

# Placeholder for future common utilities
# e.g., advanced date/time parsing, data validation, etc.

if __name__ == '__main__':
    # Example usage of RWLock
    async def reader(rw_lock, reader_id, delay):
        print(f"Reader {reader_id} trying to acquire read lock...")
        async with rw_lock.read_lock():
            print(f"Reader {reader_id} acquired read lock. Reading...")
            await asyncio.sleep(delay)
            print(f"Reader {reader_id} finished reading. Releasing read lock.")

    async def writer(rw_lock, writer_id, delay):
        print(f"Writer {writer_id} trying to acquire write lock...")
        async with rw_lock.write_lock():
            print(f"Writer {writer_id} acquired write lock. Writing...")
            await asyncio.sleep(delay)
            print(f"Writer {writer_id} finished writing. Releasing write lock.")

    async def main_rwlock_test():
        rw_lock = RWLock()
        # Start multiple readers and writers
        # Scenario: Readers start, then a writer, then more readers.
        # The writer should wait for initial readers.
        # Subsequent readers should wait for the writer.
        tasks = [
            reader(rw_lock, 1, 1),
            reader(rw_lock, 2, 1),
            writer(rw_lock, 'A', 2), # Writer 'A'
            reader(rw_lock, 3, 0.5), # This reader should wait for Writer A
            writer(rw_lock, 'B', 1), # Writer 'B' should wait for Reader 3
            reader(rw_lock, 4, 0.5), # This reader should wait for Writer B
        ]
        await asyncio.gather(*tasks)

        print("\n--- Test Writer Priority ---")
        rw_lock_priority = RWLock()
        # Scenario: A reader holds the lock, multiple writers queue up, then readers queue up.
        # Writers should get priority over new readers.
        async def reader_p(lock, id, sleep_before, sleep_during):
            await asyncio.sleep(sleep_before)
            print(f"Reader {id} attempting to acquire.")
            async with lock.read_lock():
                print(f"Reader {id} acquired.")
                await asyncio.sleep(sleep_during)
                print(f"Reader {id} releasing.")

        async def writer_p(lock, id, sleep_before, sleep_during):
            await asyncio.sleep(sleep_before)
            print(f"Writer {id} attempting to acquire.")
            async with lock.write_lock():
                print(f"Writer {id} acquired.")
                await asyncio.sleep(sleep_during)
                print(f"Writer {id} releasing.")

        tasks_priority = [
            reader_p(rw_lock_priority, "R1", 0, 2),      # R1 acquires first
            writer_p(rw_lock_priority, "W1", 0.1, 1),    # W1 queues
            writer_p(rw_lock_priority, "W2", 0.2, 1),    # W2 queues
            reader_p(rw_lock_priority, "R2", 0.3, 0.5),  # R2 queues, should wait for W1 and W2
            reader_p(rw_lock_priority, "R3", 0.4, 0.5),  # R3 queues, should wait for W1 and W2
            writer_p(rw_lock_priority, "W3", 0.5, 1),    # W3 queues
        ]
        await asyncio.gather(*tasks_priority)


    # Example usage of ThreadSafeCounter
    counter = ThreadSafeCounter(10)
    print(f"\nInitial counter value: {counter.get_value()}")
    counter.increment()
    print(f"After increment: {counter.get_value()}")
    counter.increment(5)
    print(f"After increment by 5: {counter.get_value()}")
    counter.decrement()
    print(f"After decrement: {counter.get_value()}")
    counter.set_value(100)
    print(f"After setting to 100: {counter.get_value()}")

    # Example usage of KEM ID functions
    try:
        kem_id = generate_kem_id("user_settings", "user123", "theme_preference")
        print(f"\nGenerated KEM ID: {kem_id}")
        parsed_kem_id = parse_kem_id(kem_id)
        print(f"Parsed KEM ID: {parsed_kem_id}")

        print("\nTrying to parse KEM ID with empty entity_id (expected error): system_config::global:max_users")
        try:
            parse_kem_id("system_config::global:max_users")
        except ValueError as e:
            print(f"Caught expected error: {e}")

        print(f"\nTrying to parse invalid KEM ID (not enough parts): profile-user456-displayName")
        try:
            parse_kem_id("profile-user456-displayName")
        except ValueError as e:
            print(f"Caught expected error: {e}")

        print(f"\nTrying to parse invalid KEM ID (empty parts): :::")
        try:
            parse_kem_id(":::")
        except ValueError as e:
            print(f"Caught expected error: {e}")

    except ValueError as e:
        # Should only catch errors from generate_kem_id if inputs are bad there
        print(f"Error during KEM ID generation or unexpected parsing error: {e}")

    # Run RWLock tests if this file is executed directly
    # Example:
    # import logging
    # logging.basicConfig(level=logging.INFO) # To see RWLock warnings
    # asyncio.run(main_rwlock_test())

    print("\nFinished common utils demonstrations.")
