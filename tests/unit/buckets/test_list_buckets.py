"""Unit tests for list buckets."""
from s3_tools import list_buckets

from tests.unit.conftest import create_buckets


class TestList:

    def test_list_buckets_empty(self, s3_client):
        buckets = list_buckets()

        assert len(buckets) == 0

    def test_list_buckets(self, s3_client):
        with create_buckets(s3_client, ['bucketA', 'bucketB', 'bucketC']):
            buckets = list_buckets()

        assert len(buckets) == 3

    def test_list_buckets_with_filter(self, s3_client):
        with create_buckets(s3_client, ['bucketA', 'bucketB', 'bucketC', 'ThisIsTheBucket']):
            buckets = list_buckets('*IsThe*')

        assert len(buckets) == 1
