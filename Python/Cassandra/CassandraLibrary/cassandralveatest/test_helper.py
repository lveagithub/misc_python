from cassandralvea import helper

def test_helper():
    cassandraHelper = helper.CassandraHelper(version = "1.0")
    assert cassandraHelper is not None