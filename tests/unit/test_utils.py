"""Unit tests for utils.py"""
from concurrent import futures

from s3_tools import object_exists
from s3_tools.utils import _get_future_output

from tests.unit.conftest import (
    create_bucket,
    BUCKET_NAME,
    FILENAME
)


class TestUtils:

    def test_check_nonexisting_bucket(self, s3_client):
        assert object_exists(BUCKET_NAME, "prefix/key.csv") is False

    def test_check_nonexisting_object(self, s3_client):
        with create_bucket(s3_client, BUCKET_NAME):
            response = object_exists(BUCKET_NAME, "prefix/key.csv")

        assert response is False

    def test_check_existing_object(self, s3_client):
        key = "prefix/key.csv"
        with create_bucket(s3_client, BUCKET_NAME, keys_paths=[(key, FILENAME)]):
            response = object_exists(BUCKET_NAME, key)

        assert response is True


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
