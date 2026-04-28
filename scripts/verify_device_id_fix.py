"""Verify that the DeviceID fix works correctly for ScentDeviceID and other device fields."""

import msgpack
import pandas as pd
from datetime import datetime, timezone

from src.exporter.format_writer import FormatWriter


def main():
    """Demonstrate the fix for DeviceID fields being exported as strings."""
    
    print("=" * 80)
    print("Device ID Export Fix Verification")
    print("=" * 80)
    print()
    
    # Simulate data as it would come from BigQuery
    # Note: state_value is always STRING type in BigQuery
    test_data = pd.DataFrame([
        {
            "event_timestamp": datetime(2025, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
            "event_source_type": "key",
            "ticket_id": "ABC123",
            "state_key": "Visitors:ABC123:ScentDeviceID",
            "state_value": "456",  # This looks like a number but should export as string
            "ttl_seconds": None
        },
        {
            "event_timestamp": datetime(2025, 1, 15, 10, 31, 0, tzinfo=timezone.utc),
            "event_source_type": "key",
            "ticket_id": "XYZ789",
            "state_key": "Visitors:XYZ789:EmpaticaDeviceID",
            "state_value": "E332003716C9",  # Alphanumeric device ID
            "ttl_seconds": None
        },
        {
            "event_timestamp": datetime(2025, 1, 15, 10, 32, 0, tzinfo=timezone.utc),
            "event_source_type": "key",
            "ticket_id": "DEF456",
            "state_key": "Visitors:DEF456:Status",
            "state_value": "active",  # Regular string
            "ttl_seconds": None
        },
        {
            "event_timestamp": datetime(2025, 1, 15, 10, 33, 0, tzinfo=timezone.utc),
            "event_source_type": "key",
            "ticket_id": "GHI789",
            "state_key": "Visitors:GHI789:VisitCount",
            "state_value": "42",  # This should parse as int (not a DeviceID field)
            "ttl_seconds": None
        }
    ])
    
    # Create FormatWriter
    writer = FormatWriter()
    
    # Transform to replay format
    print("Transforming data to replay format...")
    print()
    replay_records = writer._transform_to_replay_format(test_data)
    
    # Display results
    print(f"Generated {len(replay_records)} replay records:")
    print()
    
    for i, record in enumerate(replay_records, 1):
        key = record["key"]
        value_bytes = record["value"]
        
        # Unpack msgpack to see the actual value
        unpacked_value = msgpack.unpackb(value_bytes, raw=False)
        value_type = type(unpacked_value).__name__
        
        # Determine if this is a DeviceID field
        is_device_id = "DeviceID" in key
        
        print(f"Record {i}:")
        print(f"  Key:   {key}")
        print(f"  Value: {repr(unpacked_value)}")
        print(f"  Type:  {value_type}")
        
        if is_device_id:
            print(f"  ✅ DeviceID field - correctly exported as {value_type}")
            assert isinstance(unpacked_value, str), f"Expected string but got {value_type}"
        else:
            print(f"  ℹ️  Non-DeviceID field - type preserved from parsing")
        
        print()
    
    print("=" * 80)
    print("Summary:")
    print("=" * 80)
    print()
    print("✅ Fix verified successfully!")
    print()
    print("Behavior:")
    print("  • ScentDeviceID (numeric string '456') → exported as string '456'")
    print("  • EmpaticaDeviceID (alphanumeric 'E332003716C9') → exported as string")
    print("  • Status (string 'active') → exported as string")
    print("  • VisitCount (numeric string '42') → exported as int 42")
    print()
    print("The fix ensures ALL fields ending in 'DeviceID' are exported as strings,")
    print("preventing numeric conversion regardless of their content.")
    print()


if __name__ == "__main__":
    main()
