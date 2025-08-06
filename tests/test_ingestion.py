from modules.ingestion import ingest_raw_data, is_already_processed, mark_as_processed
from modules.ingestion import CHECKPOINT_FILE


def test_already_processed(tmp_path):
    with open(CHECKPOINT_FILE, "w") as fp:
        fp.write("foo\nbar\n")
    
    assert is_already_processed("foo")
    assert not is_already_processed("baz")

def test_mark_processed(tmp_path):
    mark_as_processed("x")
    with open(CHECKPOINT_FILE, "r") as fp:
        data = fp.read().splitlines()
        assert "x" in data

