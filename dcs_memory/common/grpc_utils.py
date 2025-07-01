import grpc
import time
import random
from functools import wraps
import logging
from typing import Optional, Callable, Any # For type hints

# Attempt to import pybreaker, make it optional if not strictly needed everywhere
try:
    import pybreaker
except ImportError:
    pybreaker = None # type: ignore

logger = logging.getLogger(__name__) # Logger for this module

# Standard gRPC error codes that are generally safe to retry on
RETRYABLE_ERROR_CODES = (
    grpc.StatusCode.UNAVAILABLE,       # Service temporarily unavailable
    grpc.StatusCode.DEADLINE_EXCEEDED, # Response deadline exceeded
    grpc.StatusCode.INTERNAL,          # Internal server error (can sometimes be transient)
    grpc.StatusCode.RESOURCE_EXHAUSTED # Server resources temporarily exhausted
    # grpc.StatusCode.UNKNOWN          # Unknown error, retrying might be risky or unhelpful
)

# Default parameters for retry logic
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_INITIAL_DELAY_S = 1.0  # seconds
DEFAULT_BACKOFF_FACTOR = 2.0
DEFAULT_JITTER_FRACTION = 0.1 # 10% jitter, e.g., for a 1s delay, jitter is +/- 0.1s

def retry_grpc_call(max_attempts: int = DEFAULT_MAX_ATTEMPTS,
                    initial_delay_s: float = DEFAULT_INITIAL_DELAY_S,
                    backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
                    jitter_fraction: float = DEFAULT_JITTER_FRACTION,
                    retryable_error_codes: tuple = RETRYABLE_ERROR_CODES,
                    circuit_breaker: Optional[pybreaker.CircuitBreaker] = None, # type: ignore
                    cb_enabled: bool = True):
                    # cb_enabled allows config to globally disable CBs even if one is passed
    """
    Decorator to automatically retry a gRPC call, with optional circuit breaker protection.

    :param max_attempts: Maximum number of call attempts.
    :param initial_delay_s: Initial delay before the first retry, in seconds.
    :param backoff_factor: Multiplier for an exponential increase in delay between retries.
    :param jitter_fraction: Fraction (0.0 to 1.0) to add random jitter to the delay.
                            For example, 0.1 means the actual delay will be the calculated
                            delay +/- up to 10% of that calculated delay.
    :param retryable_error_codes: A tuple of gRPC status codes that should trigger a retry.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_delay_s = initial_delay_s

            # Prepare the function to be called, potentially wrapped by circuit breaker
            protected_func: Callable[..., Any] = func
            if pybreaker and circuit_breaker and cb_enabled:
                # Define a wrapper that can be called by the circuit breaker
                def cb_wrapper(*f_args, **f_kwargs):
                    return func(*f_args, **f_kwargs)
                protected_func = lambda *p_args, **p_kwargs: circuit_breaker.call(cb_wrapper, *p_args, **p_kwargs)
                # Note: pybreaker's call method itself handles exceptions and CB state.
                # If it throws CircuitBreakerError, we might want to not retry, or retry differently.
                # For now, if CircuitBreakerError is raised, it will bypass the gRPC retry logic.

            for attempt in range(1, max_attempts + 1):
                try:
                    # Call the (potentially circuit-breaker-protected) function
                    return protected_func(*args, **kwargs)
                except pybreaker.CircuitBreakerError as cbe:
                    # If circuit is open, log and re-raise immediately. No retries.
                    logger.error(f"Circuit breaker open for {func.__name__}. Call aborted. Error: {cbe}")
                    raise # Re-raise to signal failure upwards
                except grpc.RpcError as e:
                    rpc_code = e.code() if hasattr(e, 'code') and callable(e.code) else None

                    if rpc_code in retryable_error_codes:
                        if attempt == max_attempts:
                            logger.error(
                                f"gRPC call to {func.__name__} failed after {max_attempts} attempts. "
                                f"Last error: {rpc_code} - {e.details()}",
                                exc_info=False # Do not log full stack trace for the last expected retryable error
                            )
                            raise  # Re-raise the last RpcError if all attempts are exhausted

                        # Calculate jitter: random value between -jitter_fraction and +jitter_fraction
                        jitter_value = random.uniform(-jitter_fraction, jitter_fraction) * current_delay_s
                        actual_delay_s = max(0, current_delay_s + jitter_value) # Ensure delay is not negative

                        logger.warning(
                            f"gRPC call to {func.__name__} failed (attempt {attempt}/{max_attempts}) "
                            f"with status {rpc_code}. Retrying in {actual_delay_s:.2f}s. "
                            f"Details: {e.details()}",
                            exc_info=False # Do not log full stack trace for every retry attempt
                        )
                        time.sleep(actual_delay_s)
                        current_delay_s *= backoff_factor
                    else:
                        # If the error code is not in retryable_error_codes, re-raise it immediately
                        logger.error(
                            f"gRPC call to {func.__name__} failed with non-retryable status "
                            f"{rpc_code if rpc_code else 'N/A'}. "
                            f"Details: {e.details() if hasattr(e, 'details') and callable(e.details) else str(e)}",
                            exc_info=True # Log full stack trace for non-retryable errors
                        )
                        raise
                except Exception as e_generic: # Catch other potential exceptions (e.g., network issues before gRPC layer)
                    logger.error(
                        f"A non-gRPC error occurred during call to {func.__name__} (attempt {attempt}/{max_attempts}): {e_generic}",
                        exc_info=True
                    )
                    if attempt == max_attempts:
                        raise # Re-raise if this is the last attempt

                    # For non-gRPC errors, apply similar delay logic before retrying
                    jitter_value = random.uniform(-jitter_fraction, jitter_fraction) * current_delay_s
                    actual_delay_s = max(0, current_delay_s + jitter_value)
                    logger.info(f"Retrying {func.__name__} after non-gRPC error in {actual_delay_s:.2f}s.")
                    time.sleep(actual_delay_s)
                    current_delay_s *= backoff_factor

            # This part should not be reached if max_attempts >= 1,
            # as the loop will either return a successful result or raise an exception.
    logger.error(f"Sync gRPC call to {func.__name__} exhausted attempts without success or specific error handling.")
            return None # Should ideally not happen with proper error raising.
        return wrapper
    return decorator

def async_retry_grpc_call(max_attempts: int = DEFAULT_MAX_ATTEMPTS,
                          initial_delay_s: float = DEFAULT_INITIAL_DELAY_S,
                          backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
                          jitter_fraction: float = DEFAULT_JITTER_FRACTION,
                          retryable_error_codes: tuple = RETRYABLE_ERROR_CODES,
                          circuit_breaker: Optional[pybreaker.CircuitBreaker] = None, # type: ignore
                          cb_enabled: bool = True):
    """
    Decorator to automatically retry an ASYNCHRONOUS gRPC call, with optional circuit breaker.
    """
    def decorator(async_func: Callable[..., Any]):
        @wraps(async_func)
        async def wrapper(*args, **kwargs):
            current_delay_s = initial_delay_s

            # Dynamically import AioRpcError as it's specific to grpc.aio
            AioRpcError: Any = grpc.RpcError # Default fallback
            try:
                from grpc.aio import AioRpcError as ActualAioRpcError
                AioRpcError = ActualAioRpcError
            except ImportError:
                logger.warning("grpc.aio.AioRpcError not found. Async retry might not catch specific async gRPC errors precisely.")

            protected_async_func: Callable[..., Any] = async_func
            if pybreaker and circuit_breaker and cb_enabled:
                # pybreaker's call_async expects an awaitable, so async_func is fine directly
                protected_async_func = lambda *p_args, **p_kwargs: circuit_breaker.call_async(async_func, *p_args, **p_kwargs)

            for attempt in range(1, max_attempts + 1):
                try:
                    return await protected_async_func(*args, **kwargs)
                except pybreaker.CircuitBreakerError as cbe:
                    logger.error(f"Circuit breaker open for async {async_func.__name__}. Call aborted. Error: {cbe}")
                    raise
                except AioRpcError as e: # Catch AioRpcError or fallback RpcError
                    rpc_code = e.code() if hasattr(e, 'code') and callable(e.code) else None
                    if rpc_code in retryable_error_codes:
                        if attempt == max_attempts:
                            logger.error(
                                f"Async gRPC call to {async_func.__name__} failed after {max_attempts} attempts. "
                                f"Last error: {rpc_code} - {e.details()}", exc_info=False
                            )
                            raise
                        jitter_value = random.uniform(-jitter_fraction, jitter_fraction) * current_delay_s
                        actual_delay_s = max(0, current_delay_s + jitter_value)
                        logger.warning(
                            f"Async gRPC call to {async_func.__name__} failed (attempt {attempt}/{max_attempts}) "
                            f"with status {rpc_code}. Retrying in {actual_delay_s:.2f}s. "
                            f"Details: {e.details()}", exc_info=False
                        )
                        await asyncio.sleep(actual_delay_s)
                        current_delay_s *= backoff_factor
                    else:
                        logger.error(
                            f"Async gRPC call to {async_func.__name__} failed with non-retryable status "
                            f"{rpc_code if rpc_code else 'N/A'}. Details: {e.details() if hasattr(e, 'details') and callable(e.details) else str(e)}",
                            exc_info=True
                        )
                        raise
                except Exception as e_generic: # Catch other errors
                    # If this was a CircuitBreakerError from a non-pybreaker source, it would be caught here.
                    # However, we explicitly catch pybreaker.CircuitBreakerError above.
                    logger.error(
                        f"A non-gRPC error occurred during async call to {async_func.__name__} (attempt {attempt}/{max_attempts}): {e_generic}",
                        exc_info=True
                    )
                    if attempt == max_attempts:
                        raise
                    jitter_value = random.uniform(-jitter_fraction, jitter_fraction) * current_delay_s
                    actual_delay_s = max(0, current_delay_s + jitter_value)
                    logger.info(f"Retrying async {async_func.__name__} after non-gRPC error in {actual_delay_s:.2f}s.")
                    await asyncio.sleep(actual_delay_s)
                    current_delay_s *= backoff_factor

            # Should be unreachable if max_attempts >=1
            logger.error(f"Async gRPC call to {async_func.__name__} exhausted attempts without returning or raising.")
            raise RuntimeError(f"Async call {async_func.__name__} failed after all retries.") # Ensure something is raised
        return wrapper
    return decorator


# Example of how to use the decorator (for testing or reference)
if __name__ == '__main__':
    import asyncio # Ensure asyncio is imported for async examples
    # Assume grpc.aio is available for AioRpcError testing
    try:
        from grpc.aio import AioRpcError
    except ImportError:
        AioRpcError = grpc.RpcError # Fallback for environments without grpc.aio, for basic testing

    if pybreaker is None:
        print("pybreaker not installed, circuit breaker tests will be skipped/limited.")

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    class MockStub:
        def __init__(self):
            self.call_count = 0

        # @retry_grpc_call(max_attempts=3, initial_delay_s=0.1)
        def SomeRpcMethod(self, request, timeout=None):
            self.call_count += 1
            logger.info(f"MockStub.SomeRpcMethod called, attempt {self.call_count}. Request: {request}, Timeout: {timeout}")
            if self.call_count < 2:
                raise grpc.RpcError("Simulated UNAVAILABLE") # Generic RpcError
                # To simulate specific codes:
                # mock_error = grpc.RpcError("Simulated UNAVAILABLE")
                # mock_error.code = lambda: grpc.StatusCode.UNAVAILABLE # type: ignore
                # mock_error.details = lambda: "Service is temporarily down" # type: ignore
                # raise mock_error
            elif self.call_count < 3:
                 # Simulate a non-gRPC error
                 raise ValueError("A simulated value error in RPC method")
            return f"Success on attempt {self.call_count} with request: {request}"

    # Need to wrap the method of an instance or a standalone function
    mock_stub_instance = MockStub()

    # Apply decorator directly for testing
    @retry_grpc_call(max_attempts=4, initial_delay_s=0.2, retryable_error_codes=(grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.INTERNAL))
    def decorated_rpc_call(stub_instance, request_data, timeout_val):
        # Simulate RpcError with code and details
        if stub_instance.call_count < 2: # Fail first time
            stub_instance.call_count +=1
            logger.info(f"Simulating UNAVAILABLE on attempt {stub_instance.call_count}")
            e = grpc.RpcError("Simulated UNAVAILABLE")
            e.code = lambda: grpc.StatusCode.UNAVAILABLE # type: ignore
            e.details = lambda: "Service is temporarily down" # type: ignore
            raise e
        elif stub_instance.call_count <3: # Fail second time with non-retryable
            stub_instance.call_count +=1
            logger.info(f"Simulating ABORTED on attempt {stub_instance.call_count}")
            e = grpc.RpcError("Simulated ABORTED")
            e.code = lambda: grpc.StatusCode.ABORTED # type: ignore
            e.details = lambda: "Operation aborted by client" # type: ignore
            raise e
        stub_instance.call_count +=1
        logger.info(f"Call successful on attempt {stub_instance.call_count}")
        return f"Success: {request_data} on attempt {stub_instance.call_count}"


    print("\n--- Test 1: Call succeeds after retries on UNAVAILABLE ---")
    mock_stub_instance.call_count = 0 # Reset for test
    try:
        # Re-decorate for this specific test if needed or use a new instance
        @retry_grpc_call(max_attempts=3, initial_delay_s=0.1)
        def test_rpc_1(data):
            mock_stub_instance.call_count += 1
            if mock_stub_instance.call_count < 3:
                logger.info(f"test_rpc_1: Simulating UNAVAILABLE (call {mock_stub_instance.call_count})")
                e = grpc.RpcError("Simulated UNAVAILABLE")
                e.code = lambda: grpc.StatusCode.UNAVAILABLE # type: ignore
                e.details = lambda: "Service down" # type: ignore
                raise e
            logger.info(f"test_rpc_1: Success (call {mock_stub_instance.call_count})")
            return f"Success: {data}"

        result = test_rpc_1("request_data_1")
        print(f"Test 1 Result: {result}")
    except Exception as e:
        print(f"Test 1 Error: {e}")

    print("\n--- Test 2: Call fails due to non-retryable error ---")
    mock_stub_instance.call_count = 0
    try:
        @retry_grpc_call(max_attempts=3, initial_delay_s=0.1)
        def test_rpc_2(data):
            mock_stub_instance.call_count += 1
            logger.info(f"test_rpc_2: Simulating PERMISSION_DENIED (call {mock_stub_instance.call_count})")
            e = grpc.RpcError("Simulated PERMISSION_DENIED")
            e.code = lambda: grpc.StatusCode.PERMISSION_DENIED # type: ignore
            e.details = lambda: "Auth failed" # type: ignore
            raise e
        result = test_rpc_2("request_data_2")
        print(f"Test 2 Result: {result}")
    except grpc.RpcError as e:
        print(f"Test 2 Expected RpcError: {e.code()} - {e.details()}")
    except Exception as e:
        print(f"Test 2 Unexpected Error: {e}")

    print("\n--- Test 3: Call fails after max attempts ---")
    mock_stub_instance.call_count = 0
    try:
        @retry_grpc_call(max_attempts=2, initial_delay_s=0.1)
        def test_rpc_3(data):
            mock_stub_instance.call_count += 1
            logger.info(f"test_rpc_3: Simulating INTERNAL (call {mock_stub_instance.call_count})")
            e = grpc.RpcError("Simulated INTERNAL")
            e.code = lambda: grpc.StatusCode.INTERNAL # type: ignore
            e.details = lambda: "Server hiccup" # type: ignore
            raise e
        result = test_rpc_3("request_data_3")
        print(f"Test 3 Result: {result}")
    except grpc.RpcError as e:
        print(f"Test 3 Expected RpcError after retries: {e.code()} - {e.details()}")
    except Exception as e:
        print(f"Test 3 Unexpected Error: {e}")

    print("\n--- Test 4: Non-gRPC error ---")
    mock_stub_instance.call_count = 0
    try:
        @retry_grpc_call(max_attempts=2, initial_delay_s=0.1)
        def test_rpc_4(data):
            mock_stub_instance.call_count += 1
            if mock_stub_instance.call_count < 2:
                 logger.info(f"test_rpc_4: Simulating ValueError (call {mock_stub_instance.call_count})")
                 raise ValueError("A non-gRPC error")
            logger.info(f"test_rpc_4: Success (call {mock_stub_instance.call_count})")
            return f"Success: {data}"
        result = test_rpc_4("request_data_4")
        print(f"Test 4 Result: {result}") # Should succeed on 2nd attempt
    except Exception as e:
        print(f"Test 4 Error: {e}")

```
