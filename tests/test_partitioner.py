from chronostore.core import Partitioner

def test_partition_path(tmp_path):
    p = Partitioner(tmp_path)
    path = p.get_partition_path("ES", "2025-06-14")
    assert path == tmp_path / "ES" / "2025-06-14"
