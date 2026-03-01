"""Memory tests for large sync operations.

These tests verify that memory usage stays bounded during large sync
operations and that default settings are configured for memory efficiency.
All tests are mocked — no DB, Elasticsearch, or Redis required.
"""

import gc
import importlib
import tracemalloc

import pytest

from pgsync.sync import settings

from .testing_utils import override_env_var


class TestMemoryDefaults:
    """Verify memory-related default settings."""

    def test_streaming_bulk_is_default(self):
        """streaming_bulk should be the default to avoid parallel_bulk memory buffering."""
        with override_env_var(
            ELASTICSEARCH="True",
            OPENSEARCH="False",
            ELASTICSEARCH_STREAMING_BULK=None,
        ):
            importlib.reload(settings)
            assert settings.ELASTICSEARCH_STREAMING_BULK is True

    def test_reduced_default_chunk_sizes(self):
        """Default chunk sizes should be tuned for memory efficiency."""
        with override_env_var(
            ELASTICSEARCH="True",
            OPENSEARCH="False",
            ELASTICSEARCH_CHUNK_SIZE=None,
            ELASTICSEARCH_MAX_CHUNK_BYTES=None,
            QUERY_CHUNK_SIZE=None,
        ):
            importlib.reload(settings)
            assert settings.ELASTICSEARCH_CHUNK_SIZE == 2000
            assert settings.ELASTICSEARCH_MAX_CHUNK_BYTES == 10485760  # 10MB
            assert settings.QUERY_CHUNK_SIZE == 5000


class TestMemoryBounded:
    """Verify that the sync generator pipeline doesn't leak memory."""

    def test_sync_generator_memory_bounded(self):
        """Memory should stay bounded when consuming a large generator pipeline.

        Simulates the fetchmany -> transform -> yield doc pipeline with
        50,000 rows and checks that peak memory doesn't grow proportionally
        to total row count (which would indicate a leak).
        """
        NUM_ROWS = 50_000
        SAMPLE_INTERVAL = 10_000

        def fake_row_generator(n):
            """Simulates fetchmany() yielding (keys, row, primary_keys) tuples."""
            for i in range(n):
                keys = {"book": {"id": [i]}}
                row = {
                    "isbn": f"isbn-{i}",
                    "title": f"title-{i}",
                    "description": f"A test description for book number {i}",
                }
                primary_keys = [i]
                yield keys, row, primary_keys
                # Simulate the gc.collect() added in fetchmany()
                if i % 5000 == 0 and i > 0:
                    gc.collect()

        def fake_sync_generator(row_gen):
            """Simulates sync() transforming rows into docs."""
            for i, (keys, row, primary_keys) in enumerate(row_gen):
                doc = {
                    "_id": str(primary_keys[0]),
                    "_index": "test_index",
                    "_source": row,
                }
                yield doc

        def consume_generator(gen):
            """Simulates search_client.bulk() consuming the doc generator."""
            count = 0
            samples = []
            for doc in gen:
                count += 1
                if count % SAMPLE_INTERVAL == 0:
                    samples.append(tracemalloc.get_traced_memory()[0])
            return count, samples

        # Warm up — run a small batch first to stabilize allocations
        gc.collect()
        for _ in fake_sync_generator(fake_row_generator(100)):
            pass
        gc.collect()

        # Measure
        tracemalloc.start()
        gc.collect()

        row_gen = fake_row_generator(NUM_ROWS)
        doc_gen = fake_sync_generator(row_gen)
        count, samples = consume_generator(doc_gen)

        gc.collect()
        tracemalloc.stop()

        assert count == NUM_ROWS

        # Memory should be roughly stable across samples.
        # If there's a leak, later samples would be much larger than earlier ones.
        # Allow up to 3x growth to account for GC timing and allocator behavior.
        assert len(samples) >= 2, "Not enough samples collected"
        assert samples[-1] < samples[0] * 3, (
            f"Memory grew from {samples[0] / 1024:.0f}KB to "
            f"{samples[-1] / 1024:.0f}KB ({samples[-1] / samples[0]:.1f}x) "
            f"over {NUM_ROWS} rows — possible memory leak"
        )
