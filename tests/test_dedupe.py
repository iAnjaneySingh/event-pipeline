import fakeredis

from pipeline.dedupe import DedupeStore


def test_dedupe_flow():
    store = DedupeStore.__new__(DedupeStore)
    store.r = fakeredis.FakeRedis(decode_responses=True)

    assert store.already_processed("evt-1") is False
    store.mark_processed("evt-1")
    assert store.already_processed("evt-1") is True
    assert store.already_processed("evt-2") is False
