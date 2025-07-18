from chronostore.core import Writer

def test_writer_appends(tmp_path):
    path = tmp_path / "data.bin"
    w = Writer(str(path))
    w.append(b"abcdef")
    w.flush()
    with open(path, "rb") as f:
        content = f.read()
    assert content == b"abcdef"