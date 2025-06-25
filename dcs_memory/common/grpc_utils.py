import grpc
import time
import random
import logging
from functools import wraps
import typing

# Получаем logger для этого модуля
logger = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES = [
    grpc.StatusCode.UNAVAILABLE,
    grpc.StatusCode.DEADLINE_EXCEEDED,
    # grpc.StatusCode.RESOURCE_EXHAUSTED,
]

JITTER_FRACTION = 0.1

def retry_grpc_call(func: typing.Callable) -> typing.Callable:
    """
    Декоратор для повторных попыток вызова gRPC методов экземпляра класса.
    Использует атрибуты self экземпляра (retry_max_attempts, retry_initial_delay_s,
    retry_backoff_factor) для конфигурации retry.
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs): # self - это экземпляр класса, например, GLMClient
        attempts = 0

        max_attempts = getattr(self, 'retry_max_attempts', 3)
        current_delay_s = getattr(self, 'retry_initial_delay_s', 1.0)
        backoff_factor = getattr(self, 'retry_backoff_factor', 2.0)

        last_exception = None

        while attempts < max_attempts:
            attempts += 1
            try:
                return func(self, *args, **kwargs)
            except grpc.RpcError as e:
                last_exception = e
                if e.code() in RETRYABLE_STATUS_CODES:
                    if attempts < max_attempts:
                        jitter = random.uniform(-JITTER_FRACTION * current_delay_s, JITTER_FRACTION * current_delay_s)
                        actual_delay = max(0.1, current_delay_s + jitter)

                        logger.warning(
                            f"Retry: RPC вызов {self.__class__.__name__}.{func.__name__} "
                            f"не удался (попытка {attempts}/{max_attempts}) с ошибкой {e.code()}. "
                            f"Повтор через {actual_delay:.2f}с."
                        )
                        time.sleep(actual_delay)
                        current_delay_s *= backoff_factor
                    else:
                        logger.error(
                            f"Retry: RPC вызов {self.__class__.__name__}.{func.__name__} "
                            f"не удался после {max_attempts} попыток. Последняя ошибка: {e.code()} - {e.details()}."
                        )
                        raise
                else:
                    logger.error(
                        f"Retry: RPC вызов {self.__class__.__name__}.{func.__name__} "
                        f"не удался с не подлежащей повтору ошибкой: {e.code()} - {e.details()}."
                    )
                    raise
            except Exception as e_other:
                logger.error(
                    f"Retry: Не-gRPC ошибка при вызове {self.__class__.__name__}.{func.__name__}: {e_other}",
                    exc_info=True
                )
                raise

        if last_exception:
             raise last_exception
        return None # Недостижимо, если max_attempts > 0
    return wrapper
