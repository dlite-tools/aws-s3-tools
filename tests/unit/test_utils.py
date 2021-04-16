"""Unit tests for utils.py"""
from concurrent import futures

from s3_tools.utils import _get_future_output


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
