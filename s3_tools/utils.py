"""General utilities."""
from typing import Any
from concurrent import futures


def _get_future_output(future: futures.Future) -> Any:
    """Get a futures.Future result or exception message.

    Parameters
    ----------
    future : futures.Future
        An async callable method.

    Returns
    -------
    Any
        If execution has no error will return the value returned by the callable method,
        else will return the exception message.
    """
    try:
        return future.result()
    except Exception as e:
        return repr(e)
