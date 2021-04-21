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


def _create_progress_bar(description: str, length: int):
    """Create a console progress bar using 'rich' package.

    Parameters
    ----------
    description : str
        Progress bar description.
    length : int
        Progress bar length.

    Returns
    -------
    Tuple[Optional[Progress], Optional[int]]
        The progress bar object and the task ID associated.
        (Need to install extra [progress] to be used)
    """
    try:
        from rich.progress import (
            BarColumn,
            Progress,
            TextColumn
        )

        progress = Progress(
            TextColumn("[bold blue]{task.description}", justify="right"),
            BarColumn(bar_width=None),
            TextColumn("[bold blue][{task.completed}/{task.total}]"),
        )

        task_id = progress.add_task(
            description=description,
            total=length,
            start=False
        )

    except ImportError:
        print("Missing extra dependency to use progress bar."
              " Please run 'pip install aws-s3-tools[progress]'.")
        raise

    return progress, task_id
