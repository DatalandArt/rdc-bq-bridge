# Device ID Export Type Fix - Changelog

## Problem

When exporting data from BigQuery to Avro format for Redis replay, fields like `ScentDeviceID`, `EmpaticaDeviceID`, and other device identifiers were being exported with numeric types (int/float) when their values looked numeric (e.g., `"123"`, `"456.5"`). This caused issues for downstream consumers expecting string types for all device IDs.

### Root Cause

In `src/exporter/format_writer.py`, the `_transform_to_replay_format()` method attempted to automatically detect and parse numeric values from the BigQuery `state_value` column (which is always stored as STRING type). The logic would convert:
- `"123"` → `123` (int)
- `"45.6"` → `45.6` (float)
- `"SCENT123"` → `"SCENT123"` (string)

This type inference worked well for general state values but caused problems for device IDs, which should **always** be treated as strings regardless of their content.

## Solution

Modified `src/exporter/format_writer.py:236-267` to add special handling for device ID fields:

```python
# Special handling: Device ID fields should always be strings
# This includes EmpaticaDeviceID, ScentDeviceID, BlueIoTDeviceID, etc.
if "DeviceID" in state_key:
    typed_value = value_str
else:
    # Normal type inference logic for other fields
    ...
```

### Behavior After Fix

| Field | Value in BQ | Old Export Type | New Export Type |
|-------|-------------|-----------------|-----------------|
| `Visitors:T001:ScentDeviceID` | `"123"` | int (123) | **string ("123")** |
| `Visitors:T002:EmpaticaDeviceID` | `"E332003716C9"` | string | string (unchanged) |
| `Visitors:T003:BlueIoTDeviceID` | `"45.6"` | float (45.6) | **string ("45.6")** |
| `Visitors:T004:Status` | `"100"` | int (100) | int (100) (unchanged) |
| `Visitors:T005:VisitCount` | `"42"` | int (42) | int (42) (unchanged) |

## Files Changed

1. **`src/exporter/format_writer.py`**
   - Added conditional check for "DeviceID" in state key name
   - Ensures all DeviceID fields bypass numeric type inference

2. **`tests/test_device_id_export.py`** (new)
   - Comprehensive test suite verifying DeviceID fields export as strings
   - Tests both in-memory transformation and full Avro file export

3. **`scripts/verify_device_id_fix.py`** (new)
   - Verification script demonstrating the fix
   - Useful for manual testing and validation

4. **`README.md`**
   - Added documentation about DeviceID type handling in export section

## Testing

All tests pass successfully:

```bash
$ python -m pytest tests/test_device_id_export.py -v
tests/test_device_id_export.py::test_device_id_fields_exported_as_strings PASSED
tests/test_device_id_export.py::test_avro_file_device_id_export PASSED
```

Manual verification:
```bash
$ python scripts/verify_device_id_fix.py
✅ Fix verified successfully!
```

## Impact

### Breaking Changes
**None** - This is a **fix**, not a breaking change:
- The upstream application should have been setting device IDs as strings all along
- BigQuery schema remains unchanged (state_value is still STRING type)
- Only affects the msgpack type encoding in exported Avro files

### Benefits
1. ✅ **Type Consistency**: Device IDs are now always strings in exported data
2. ✅ **No Ambiguity**: Eliminates guessing whether "123" should be int or string
3. ✅ **Redis Replay Compatible**: Ensures correct types when replaying to Redis
4. ✅ **Future-Proof**: Handles both numeric and alphanumeric device IDs correctly

### Recommendations for Upstream Applications

Ensure device IDs are set as strings when storing in Redis:

```python
import msgpack

# ✅ Correct - explicitly cast to string
await redis.set(
    f"Visitors:{ticket_id}:ScentDeviceID",
    msgpack.packb(str(device_id), use_bin_type=True)
)

# ❌ Incorrect - could pack as int if device_id is numeric
await redis.set(
    f"Visitors:{ticket_id}:ScentDeviceID",
    msgpack.packb(device_id, use_bin_type=True)
)
```

## Verification

To verify the fix works in your environment:

1. Export some data with ScentDeviceID fields:
   ```bash
   rdc-export export-ticket --ticket-id YOUR_TICKET --format avro
   ```

2. Check the exported Avro file:
   ```python
   import fastavro
   import msgpack
   
   with open('output.avro', 'rb') as f:
       reader = fastavro.reader(f)
       for record in reader:
           if 'ScentDeviceID' in record['key']:
               value = msgpack.unpackb(record['value'], raw=False)
               print(f"Type: {type(value)}, Value: {value}")
               assert isinstance(value, str), "Should be string!"
   ```

## Questions?

If you encounter any issues or have questions about this fix, please open an issue with:
- Sample data showing the problem
- Expected vs actual behavior
- Export command used
