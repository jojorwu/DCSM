import asyncio
import pytest
import time # For non-async sleep in some test scenarios
from unittest.mock import MagicMock

from dcs_memory.common.utils import RWLock, ThreadSafeCounter, generate_kem_id, parse_kem_id

# pytestmark = pytest.mark.asyncio # Removed global marker

@pytest.fixture
def rw_lock():
    return RWLock()

async def basic_reader(lock: RWLock, reader_id: int, start_event: asyncio.Event, end_event: asyncio.Event, duration: float = 0.01):
    # print(f"Reader {reader_id} waiting for start_event")
    await start_event.wait()
    # print(f"Reader {reader_id} trying to acquire read lock")
    async with lock.read_lock():
        # print(f"Reader {reader_id} acquired read lock")
        assert not lock._writer_active
        lock._readers_count_during_test = lock._readers_count # For assertion outside
        await asyncio.sleep(duration) # Simulate work
        # print(f"Reader {reader_id} releasing read lock")
    end_event.set()

async def basic_writer(lock: RWLock, writer_id: any, start_event: asyncio.Event, end_event: asyncio.Event, duration: float = 0.01):
    # print(f"Writer {writer_id} waiting for start_event")
    await start_event.wait()
    # print(f"Writer {writer_id} trying to acquire write lock")
    async with lock.write_lock():
        # print(f"Writer {writer_id} acquired write lock")
        assert lock._writer_active
        assert lock._readers_count == 0
        await asyncio.sleep(duration) # Simulate work
        # print(f"Writer {writer_id} releasing write lock")
    end_event.set()

@pytest.mark.asyncio
class TestRWLock:

    async def test_single_reader(self, rw_lock: RWLock):
        start_event = asyncio.Event()
        end_event = asyncio.Event()
        start_event.set()
        await basic_reader(rw_lock, 1, start_event, end_event, 0.01)
        assert end_event.is_set()
        assert rw_lock._readers_count == 0
        assert not rw_lock._writer_active

    async def test_multiple_readers_concurrently(self, rw_lock: RWLock):
        num_readers = 5
        start_event = asyncio.Event()
        end_events = [asyncio.Event() for _ in range(num_readers)]

        tasks = [
            basic_reader(rw_lock, i, start_event, end_events[i], 0.05)
            for i in range(num_readers)
        ]

        start_event.set() # Signal all readers to start
        await asyncio.gather(*tasks)

        for event in end_events:
            assert event.is_set()
        assert rw_lock._readers_count == 0
        # Check that at some point all readers were active together
        # This needs a slight modification to basic_reader or a shared counter if we want to be precise
        # For now, we rely on the fact that they all acquired the lock during their execution window.
        # A more robust test might involve checking a counter incremented inside the read lock.
        # The `lock._readers_count_during_test` is a bit of a hack for this.
        # A better way would be to pass a list to the reader to append timestamps or states.

    async def test_single_writer(self, rw_lock: RWLock):
        start_event = asyncio.Event()
        end_event = asyncio.Event()
        start_event.set()
        await basic_writer(rw_lock, 'W1', start_event, end_event, 0.01)
        assert end_event.is_set()
        assert rw_lock._readers_count == 0
        assert not rw_lock._writer_active

    async def test_writer_blocks_readers(self, rw_lock: RWLock):
        writer_start_event = asyncio.Event()
        writer_end_event = asyncio.Event()
        reader_start_event = asyncio.Event()
        reader_end_event = asyncio.Event()

        reader_acquired_lock_time = None
        writer_acquired_lock_time = None
        reader_finished_time = None
        writer_finished_time = None

        async def writer_task():
            nonlocal writer_acquired_lock_time, writer_finished_time
            # print("Writer task started")
            await writer_start_event.wait()
            # print("Writer task proceeding to acquire lock")
            async with rw_lock.write_lock():
                writer_acquired_lock_time = time.monotonic()
                # print(f"Writer acquired lock at {writer_acquired_lock_time}")
                assert rw_lock._writer_active
                assert rw_lock._readers_count == 0
                await asyncio.sleep(0.1) # Hold lock
            writer_finished_time = time.monotonic()
            # print(f"Writer finished at {writer_finished_time}")
            writer_end_event.set()

        async def reader_task():
            nonlocal reader_acquired_lock_time, reader_finished_time
            # print("Reader task started")
            await reader_start_event.wait()
            # print("Reader task proceeding to acquire lock")
            async with rw_lock.read_lock():
                reader_acquired_lock_time = time.monotonic()
                # print(f"Reader acquired lock at {reader_acquired_lock_time}")
                assert not rw_lock._writer_active
                await asyncio.sleep(0.01) # Hold lock
            reader_finished_time = time.monotonic()
            # print(f"Reader finished at {reader_finished_time}")
            reader_end_event.set()

        # Start writer first, ensure it gets the lock
        writer_task_handle = asyncio.create_task(writer_task())
        await asyncio.sleep(0.005) # give writer a head start
        writer_start_event.set()
        await asyncio.sleep(0.01) # ensure writer is trying to acquire or has acquired

        # Start reader, it should block
        reader_task_handle = asyncio.create_task(reader_task())
        await asyncio.sleep(0.005) # give reader a head start
        reader_start_event.set()


        await asyncio.wait_for(writer_end_event.wait(), timeout=1)
        await asyncio.wait_for(reader_end_event.wait(), timeout=1)

        assert writer_acquired_lock_time is not None
        assert writer_finished_time is not None
        assert reader_acquired_lock_time is not None

        # Key assertion: Reader should acquire lock only after writer has finished
        assert reader_acquired_lock_time > writer_finished_time

    async def test_reader_blocks_writer(self, rw_lock: RWLock):
        writer_start_event = asyncio.Event()
        writer_end_event = asyncio.Event()
        reader_start_event = asyncio.Event()
        reader_end_event = asyncio.Event()

        reader_acquired_lock_time = None
        writer_acquired_lock_time = None
        reader_finished_time = None
        writer_finished_time = None

        async def writer_task():
            nonlocal writer_acquired_lock_time, writer_finished_time
            await writer_start_event.wait()
            async with rw_lock.write_lock():
                writer_acquired_lock_time = time.monotonic()
                assert rw_lock._writer_active
                assert rw_lock._readers_count == 0
                await asyncio.sleep(0.01) # Hold lock
            writer_finished_time = time.monotonic()
            writer_end_event.set()

        async def reader_task():
            nonlocal reader_acquired_lock_time, reader_finished_time
            await reader_start_event.wait()
            async with rw_lock.read_lock():
                reader_acquired_lock_time = time.monotonic()
                assert not rw_lock._writer_active
                await asyncio.sleep(0.1) # Hold lock
            reader_finished_time = time.monotonic()
            reader_end_event.set()

        # Start reader first, ensure it gets the lock
        reader_task_handle = asyncio.create_task(reader_task())
        await asyncio.sleep(0.005)
        reader_start_event.set()
        await asyncio.sleep(0.01) # ensure reader is trying or has acquired

        # Start writer, it should block
        writer_task_handle = asyncio.create_task(writer_task())
        await asyncio.sleep(0.005)
        writer_start_event.set()

        await asyncio.wait_for(reader_end_event.wait(), timeout=1)
        await asyncio.wait_for(writer_end_event.wait(), timeout=1)

        assert reader_acquired_lock_time is not None
        assert reader_finished_time is not None
        assert writer_acquired_lock_time is not None

        # Key assertion: Writer should acquire lock only after reader has finished
        assert writer_acquired_lock_time > reader_finished_time

    async def test_writer_priority_over_new_readers(self, rw_lock: RWLock):
        # R1 acquires lock. W1 tries to acquire (queued). R2 tries to acquire.
        # W1 should get the lock before R2.

        log = [] # To record acquisition order

        async def reader(id, sleep_before, sleep_during):
            await asyncio.sleep(sleep_before)
            log.append(f"Reader {id} trying")
            async with rw_lock.read_lock():
                log.append(f"Reader {id} acquired")
                await asyncio.sleep(sleep_during)
            log.append(f"Reader {id} released")

        async def writer(id, sleep_before, sleep_during):
            await asyncio.sleep(sleep_before)
            log.append(f"Writer {id} trying")
            async with rw_lock.write_lock():
                log.append(f"Writer {id} acquired")
                await asyncio.sleep(sleep_during)
            log.append(f"Writer {id} released")

        # R1 starts and acquires lock
        # W1 starts and waits
        # R2 starts and waits (should wait for W1)
        # W2 starts and waits (should wait for W1, then R2 if R2 wasn't blocked by W1)

        tasks = [
            reader("R1", 0, 0.1),     # R1 acquires first
            writer("W1", 0.01, 0.1),  # W1 queues while R1 holds
            reader("R2", 0.02, 0.05), # R2 queues, should wait for W1
            writer("W2", 0.03, 0.05), # W2 queues, should wait for W1 and R2
            reader("R3", 0.04, 0.05)  # R3 queues, should wait for W1, R2, W2
        ]
        await asyncio.gather(*tasks)

        # print("Event log:", log)

        r1_acquired_idx = log.index("Reader R1 acquired")
        w1_acquired_idx = log.index("Writer W1 acquired")
        r2_acquired_idx = log.index("Reader R2 acquired")
        w2_acquired_idx = log.index("Writer W2 acquired")
        r3_acquired_idx = log.index("Reader R3 acquired")


        # R1 must acquire before W1
        assert r1_acquired_idx < w1_acquired_idx

        # W1 (writer) must acquire before W2 (writer), if W1 was queued first or at same time
        assert w1_acquired_idx < w2_acquired_idx

        # Writers (W1, W2) must acquire before later readers (R2, R3)
        assert w1_acquired_idx < r2_acquired_idx
        assert w1_acquired_idx < r3_acquired_idx
        assert w2_acquired_idx < r2_acquired_idx
        assert w2_acquired_idx < r3_acquired_idx

        # Order between R2 and R3 is not strictly guaranteed if they are notified simultaneously
        # but both must be after all prioritized writers.

        # Check release order relative to next acquisition
        r1_released_idx = log.index("Reader R1 released")
        w1_released_idx = log.index("Writer W1 released")
        # r2_released_idx = log.index("Reader R2 released") # Order of R2/R3 release not fixed
        # r3_released_idx = log.index("Reader R3 released")
        w2_released_idx = log.index("Writer W2 released")


        assert r1_released_idx < w1_acquired_idx  # R1 releases before W1 acquires
        assert w1_released_idx < w2_acquired_idx  # W1 releases before W2 acquires
        assert w2_released_idx < r2_acquired_idx  # W2 releases before R2 acquires
        assert w2_released_idx < r3_acquired_idx  # W2 releases before R3 acquires

    async def test_release_without_acquire_logs_warning(self, rw_lock: RWLock, caplog):
        # Test release_read without acquire
        await rw_lock.release_read()
        assert any("release_read called when no readers were active" in message for message in caplog.messages)
        caplog.clear()

        # Test release_write without acquire
        await rw_lock.release_write()
        assert any("release_write called when no writer was active" in message for message in caplog.messages)
        caplog.clear()

        # Test correct sequence does not log warning
        await rw_lock.acquire_read()
        await rw_lock.release_read()
        assert not any("release_read called when no readers were active" in message for message in caplog.messages)

        await rw_lock.acquire_write()
        await rw_lock.release_write()
        assert not any("release_write called when no writer was active" in message for message in caplog.messages)

class TestThreadSafeCounter:
    def test_initial_value(self):
        counter = ThreadSafeCounter(5)
        assert counter.get_value() == 5

    def test_increment(self):
        counter = ThreadSafeCounter()
        assert counter.increment() == 1
        assert counter.get_value() == 1
        assert counter.increment(3) == 4
        assert counter.get_value() == 4

    def test_decrement(self):
        counter = ThreadSafeCounter(10)
        assert counter.decrement() == 9
        assert counter.get_value() == 9
        assert counter.decrement(5) == 4
        assert counter.get_value() == 4

    def test_set_value(self):
        counter = ThreadSafeCounter()
        counter.set_value(100)
        assert counter.get_value() == 100

    # Thread safety is harder to test reliably in unit tests without hammering,
    # but the use of RLock should ensure it.
    # For a real-world scenario, stress tests would be needed.

class TestKemIdFunctions:
    def test_generate_kem_id_success(self):
        assert generate_kem_id("type", "id", "attr") == "type:id:attr"
        assert generate_kem_id("user_profile", "123", "email") == "user_profile:123:email"
        # Attribute name can have colons; type and id are simple for consistent parsing.
        assert generate_kem_id("type_simple", "id_simple", "attribute:with:colons") == "type_simple:id_simple:attribute:with:colons"


    def test_generate_kem_id_empty_parts(self):
        # Updated to match the exact error message from generate_kem_id
        with pytest.raises(ValueError, match="kem_type, entity_id, and attribute_name must all be non-empty."):
            generate_kem_id("", "id", "attr")
        with pytest.raises(ValueError, match="kem_type, entity_id, and attribute_name must all be non-empty."):
            generate_kem_id("type", "", "attr")
        with pytest.raises(ValueError, match="kem_type, entity_id, and attribute_name must all be non-empty."):
            generate_kem_id("type", "id", "")

    def test_parse_kem_id_success(self):
        assert parse_kem_id("type:id:attr") == ("type", "id", "attr")
        assert parse_kem_id("user_profile:123:email") == ("user_profile", "123", "email")
        # Attribute name can have colons
        assert parse_kem_id("type:id:attribute:name:with:colons") == ("type", "id", "attribute:name:with:colons")
        # No more test for "system_config::global:max_users" here, as empty entity_id is now invalid


    def test_parse_kem_id_invalid_format(self):
        # Test for too few parts
        with pytest.raises(ValueError, match="Invalid KEM ID format: type:id. Expected 'type:id:attribute' with at least 3 parts."):
            parse_kem_id("type:id")
        # Test for no colons
        with pytest.raises(ValueError, match="Invalid KEM ID format: typeidattr. Expected 'type:id:attribute' with at least 3 parts."):
            parse_kem_id("typeidattr")
        # Test for empty string (will fail on not all parts non-empty after split, or len(parts) !=3 if it's truly empty or just one part)
        with pytest.raises(ValueError, match="(Invalid KEM ID format: . Expected 'type:id:attribute' with at least 3 parts.)|(All parts .* must be non-empty.)"):
             parse_kem_id("")
        # Test for KEM ID that results in empty parts after split
        with pytest.raises(ValueError, match=r"Invalid KEM ID: ':::'. All parts \(type, id, attribute\) must be non-empty."):
            parse_kem_id(":::")
        with pytest.raises(ValueError, match=r"Invalid KEM ID: 'type::attr'. All parts \(type, id, attribute\) must be non-empty."):
            parse_kem_id("type::attr") # Empty entity_id
        with pytest.raises(ValueError, match=r"Invalid KEM ID: ':id:attr'. All parts \(type, id, attribute\) must be non-empty."):
            parse_kem_id(":id:attr") # Empty type
        with pytest.raises(ValueError, match=r"Invalid KEM ID: 'type:id:'. All parts \(type, id, attribute\) must be non-empty."):
            parse_kem_id("type:id:") # Empty attribute

# Example of how to run specific tests if needed, though pytest handles this.
# if __name__ == "__main__":
#     pytest.main()
