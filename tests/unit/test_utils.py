"""Unit tests for utils.py"""
import builtins
from concurrent import futures

import pytest

from s3_tools.utils import (
    _create_progress_bar,
    _get_future_output
)


@pytest.fixture
def hide_available_pkg(monkeypatch):
    import_orig = builtins.__import__

    def mocked_import(name, *args, **kwargs):
        if "rich." in name:
            raise ModuleNotFoundError()
        return import_orig(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", mocked_import)


class TestFuture:

    def foo(self, number):
        return 1 / number

    def test_get_future(self):

        with futures.ThreadPoolExecutor(max_workers=2) as executor:
            executions = {
                executor.submit(self.foo, i): {"number": i}
                for i in range(5)
            }

            responses = [
                (executions[future]["number"], _get_future_output(future))
                for future in futures.as_completed(executions)
            ]

        assert len(responses) == 5
        assert sorted(responses)[0][1] == "ZeroDivisionError('division by zero')"


class TestProgressBar:

    @pytest.mark.usefixtures("hide_available_pkg")
    def test_if_no_package(self):
        with pytest.raises(ModuleNotFoundError):
            _create_progress_bar("Test", 10)

    def test_create_progress_bar(self):
        pytest.importorskip("rich")

        progress, task_id = _create_progress_bar("Test", 10)
        print(progress, task_id)

        assert len(progress.columns) == 3
        assert task_id == 0
        assert progress.tasks[task_id].total == 10
