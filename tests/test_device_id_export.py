"""Test that device ID fields are always exported as strings in Avro format."""

import msgpack
import pandas as pd
import pytest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from src.exporter.format_writer import FormatWriter


def test_device_id_fields_exported_as_strings():
    """Test that DeviceID fields (EmpaticaDeviceID, ScentDeviceID, etc.) are exported as strings."""
    
    # Create test data with various DeviceID fields
    # Simulate data that was stored as numbers in BigQuery but should export as strings
    test_data = pd.DataFrame([
        {
            "event_timestamp": datetime(2025, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
            "event_source_type": "key",
            "ticket_id": "TICKET001",
            "state_key": "Visitors:TICKET001:EmpaticaDeviceID",
            "state_value": "123456",  # Numeric-looking string
            "ttl_seconds": None
        },
        {
            "event_timestamp": datetime(2025, 1, 15, 10, 31, 0, tzinfo=timezone.utc),
            "event_source_type": "key",
            "ticket_id": "TICKET002",
            "state_key": "Visitors:TICKET002:ScentDeviceID",
            "state_value": "789",  # Pure numeric string
            "ttl_seconds": None
        },
        {
            "event_timestamp": datetime(2025, 1, 15, 10, 32, 0, tzinfo=timezone.utc),
            "event_source_type": "key",
            "ticket_id": "TICKET003",
            "state_key": "Visitors:TICKET003:BlueIoTDeviceID",
            "state_value": "42.5",  # Float-looking string
            "ttl_seconds": None
        },
        {
            "event_timestamp": datetime(2025, 1, 15, 10, 33, 0, tzinfo=timezone.utc),
            "event_source_type": "key",
            "ticket_id": "TICKET004",
            "state_key": "Visitors:TICKET004:Status",
            "state_value": "100",  # Non-DeviceID field - should parse as int
            "ttl_seconds": None
        },
        {
            "event_timestamp": datetime(2025, 1, 15, 10, 34, 0, tzinfo=timezone.utc),
            "event_source_type": "key",
            "ticket_id": "TICKET005",
            "state_key": "Visitors:TICKET005:EmpaticaDeviceID",
            "state_value": "E332003716C9",  # Alphanumeric device ID
            "ttl_seconds": None
        }
    ])
    
    # Create FormatWriter and transform to replay format
    writer = FormatWriter()
    replay_records = writer._transform_to_replay_format(test_data)
    
    # Verify we got 5 records
    assert len(replay_records) == 5
    
    # Test 1: EmpaticaDeviceID with numeric string "123456" should be string
    record1 = replay_records[0]
    assert record1["key"] == "Visitors:TICKET001:EmpaticaDeviceID"
    unpacked1 = msgpack.unpackb(record1["value"], raw=False)
    assert unpacked1 == "123456"  # Should be string, not int
    assert isinstance(unpacked1, str)
    
    # Test 2: ScentDeviceID with numeric string "789" should be string
    record2 = replay_records[1]
    assert record2["key"] == "Visitors:TICKET002:ScentDeviceID"
    unpacked2 = msgpack.unpackb(record2["value"], raw=False)
    assert unpacked2 == "789"  # Should be string, not int
    assert isinstance(unpacked2, str)
    
    # Test 3: BlueIoTDeviceID with float string "42.5" should be string
    record3 = replay_records[2]
    assert record3["key"] == "Visitors:TICKET003:BlueIoTDeviceID"
    unpacked3 = msgpack.unpackb(record3["value"], raw=False)
    assert unpacked3 == "42.5"  # Should be string, not float
    assert isinstance(unpacked3, str)
    
    # Test 4: Status field (non-DeviceID) should parse as int
    record4 = replay_records[3]
    assert record4["key"] == "Visitors:TICKET004:Status"
    unpacked4 = msgpack.unpackb(record4["value"], raw=False)
    assert unpacked4 == 100  # Should be int
    assert isinstance(unpacked4, int)
    
    # Test 5: Alphanumeric EmpaticaDeviceID should remain string
    record5 = replay_records[4]
    assert record5["key"] == "Visitors:TICKET005:EmpaticaDeviceID"
    unpacked5 = msgpack.unpackb(record5["value"], raw=False)
    assert unpacked5 == "E332003716C9"
    assert isinstance(unpacked5, str)


def test_avro_file_device_id_export():
    """Test full Avro export to ensure DeviceID fields are strings in the actual file."""
    
    test_data = pd.DataFrame([
        {
            "event_timestamp": datetime(2025, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
            "event_source_type": "key",
            "ticket_id": "TICKET789",
            "state_key": "Visitors:TICKET789:ScentDeviceID",
            "state_value": "999",  # Numeric string that should stay as string
            "ttl_seconds": None
        }
    ])
    
    writer = FormatWriter()
    
    with TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test_export.avro"
        
        # Write Avro file
        writer.write_avro_for_replay(test_data, output_path)
        
        # Read back and verify
        import fastavro
        with open(output_path, 'rb') as f:
            reader = fastavro.reader(f)
            records = list(reader)
        
        assert len(records) == 1
        record = records[0]
        
        # Unpack the msgpack value
        unpacked_value = msgpack.unpackb(record["value"], raw=False)
        
        # Verify it's a string, not an int
        assert unpacked_value == "999"
        assert isinstance(unpacked_value, str)
        assert record["key"] == "Visitors:TICKET789:ScentDeviceID"


if __name__ == "__main__":
    # Run tests
    test_device_id_fields_exported_as_strings()
    test_avro_file_device_id_export()
    print("✅ All tests passed! DeviceID fields are correctly exported as strings.")
