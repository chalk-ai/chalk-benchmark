#!/usr/bin/env python3
"""
Convert IPC to Parquet compatible with parquet_wasm
Handles LargeList -> List conversion
"""
import base64
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

# Your base64 data
b64_data = """QVJST1cxAAD/////wA0AABAAAAAAAAoADAAGAAUACAAKAAAAAAEEAAwAAAAIAAgAAAAEAAgAAAAEAAAAIwAAAEQNAADgDAAAkAwAACwMAADMCwAAbAsAABALAAC0CgAAWAoAAPwJAACgCQAAOAkAANwIAACACAAAIAgAAMAHAABgBwAA/AYAAJwGAAA8BgAA3AUAAFAFAADEBAAAZAQAAAQEAACgAwAAPAMAANgCAAB0AgAADAIAAKQBAAA8AQAA1AAAAGwAAAAEAAAAWPP//wAAAQIQAAAATAAAAAQAAAAAAAAAOwAAAHRyYW5zYWN0aW9uLnNlbmRlcl91c2VyLnNlbmRlcl90cmFuc2FjdGlvbl9jb3VudF9fNzc3NjAwMF9fAGjz//8AAAABQAAAALzz//8AAAECEAAAAEwAAAAEAAAAAAAAADsAAAB0cmFuc2FjdGlvbi5zZW5kZXJfdXNlci5zZW5kZXJfdHJhbnNhY3Rpb25fY291bnRfXzUxODQwMDBfXwDM8///AAAAAUAAAAAg9P//AAABAhAAAABMAAAABAAAAAAAAAA7AAAAdHJhbnNhY3Rpb24uc2VuZGVyX3VzZXIuc2VuZGVyX3RyYW5zYWN0aW9uX2NvdW50X18yNTkyMDAwX18AMPT//wAAAAFAAAAAhPT//wAAAQIQAAAATAAAAAQAAAAAAAAAOgAAAHRyYW5zYWN0aW9uLnNlbmRlcl91c2VyLnNlbmRlcl90cmFuc2FjdGlvbl9jb3VudF9fNjA0ODAwX18AAJT0//8AAAABQAAAAOj0//8AAAECEAAAAEwAAAAEAAAAAAAAADkAAAB0cmFuc2FjdGlvbi5zZW5kZXJfdXNlci5zZW5kZXJfdHJhbnNhY3Rpb25fY291bnRfXzg2NDAwX18AAAD49P//AAAAAUAAAABM9f//AAABAhAAAABMAAAABAAAAAAAAAA4AAAAdHJhbnNhY3Rpb24uc2VuZGVyX3VzZXIuc2VuZGVyX3RyYW5zYWN0aW9uX2NvdW50X18zNjAwX18AAAAAXPX//wAAAAFAAAAAsPX//wAAAQMQAAAATAAAAAQAAAAAAAAAOQAAAHRyYW5zYWN0aW9uLnNlbmRlcl91c2VyLnNlbmRlcl90cmFuc2FjdGlvbl9zdW1fXzc3NzYwMDBfXwAAALL2//8AAAIAEPb//wAAAQMQAAAATAAAAAQAAAAAAAAAOQAAAHRyYW5zYWN0aW9uLnNlbmRlcl91c2VyLnNlbmRlcl90cmFuc2FjdGlvbl9zdW1fXzUxODQwMDBfXwAAABL3//8AAAIAcPb//wAAAQMQAAAATAAAAAQAAAAAAAAAOQAAAHRyYW5zYWN0aW9uLnNlbmRlcl91c2VyLnNlbmRlcl90cmFuc2FjdGlvbl9zdW1fXzI1OTIwMDBfXwAAAHL3//8AAAIA0Pb//wAAAQMQAAAATAAAAAQAAAAAAAAAOAAAAHRyYW5zYWN0aW9uLnNlbmRlcl91c2VyLnNlbmRlcl90cmFuc2FjdGlvbl9zdW1fXzYwNDgwMF9fAAAAANL3//8AAAIAMPf//wAAAQMQAAAASAAAAAQAAAAAAAAANwAAAHRyYW5zYWN0aW9uLnNlbmRlcl91c2VyLnNlbmRlcl90cmFuc2FjdGlvbl9zdW1fXzg2NDAwX18ALvj//wAAAgCM9///AAABAxAAAABIAAAABAAAAAAAAAA2AAAAdHJhbnNhY3Rpb24uc2VuZGVyX3VzZXIuc2VuZGVyX3RyYW5zYWN0aW9uX3N1bV9fMzYwMF9fAACK+P//AAACAOj3//8AAAEVFAAAAEgAAAAEAAAAAQAAAEAAAAAyAAAAdHJhbnNhY3Rpb24uc2VuZGVyX3VzZXIudG9wNV9yZWNpcGllbnRzX18yNDE5MjAwX18AAET4//9A+P//AAABAhAAAAAYAAAABAAAAAAAAAAEAAAAaXRlbQAAAAAc+P//AAAAAUAAAABw+P//AAABFRQAAABIAAAABAAAAAEAAABAAAAAMAAAAHRyYW5zYWN0aW9uLnNlbmRlcl91c2VyLnRvcDVfcmVjaXBpZW50c19fODY0MDBfXwAAAADM+P//yPj//wAAAQIQAAAAGAAAAAQAAAAAAAAABAAAAGl0ZW0AAAAApPj//wAAAAFAAAAA+Pj//wAAAQMQAAAASAAAAAQAAAAAAAAANgAAAHRyYW5zYWN0aW9uLnNlbmRlcl91c2VyLmF2ZXJhZ2VfYW1vdW50X3NlbnRfXzI1OTIwMDBfXwAA9vn//wAAAgBU+f//AAABAxAAAABIAAAABAAAAAAAAAA1AAAAdHJhbnNhY3Rpb24uc2VuZGVyX3VzZXIuYXZlcmFnZV9hbW91bnRfc2VudF9fNjA0ODAwX18AAABS+v//AAACALD5//8AAAEDEAAAAEgAAAAEAAAAAAAAADQAAAB0cmFuc2FjdGlvbi5zZW5kZXJfdXNlci5hdmVyYWdlX2Ftb3VudF9zZW50X184NjQwMF9fAAAAAK76//8AAAIADPr//wAAAQMQAAAATAAAAAQAAAAAAAAAOAAAAHRyYW5zYWN0aW9uLnNlbmRlcl91c2VyLnRvdGFsX2Ftb3VudF9yZWNlaXZlZF9fMjU5MjAwMF9fAAAAAA77//8AAAIAbPr//wAAAQMQAAAASAAAAAQAAAAAAAAANwAAAHRyYW5zYWN0aW9uLnNlbmRlcl91c2VyLnRvdGFsX2Ftb3VudF9yZWNlaXZlZF9fNjA0ODAwX18Aavv//wAAAgDI+v//AAABAxAAAABIAAAABAAAAAAAAAA2AAAAdHJhbnNhY3Rpb24uc2VuZGVyX3VzZXIudG90YWxfYW1vdW50X3JlY2VpdmVkX184NjQwMF9fAADG+///AAACACT7//8AAAEDEAAAAEgAAAAEAAAAAAAAADQAAAB0cmFuc2FjdGlvbi5zZW5kZXJfdXNlci50b3RhbF9hbW91bnRfc2VudF9fMjU5MjAwMF9fAAAAACL8//8AAAIAgPv//wAAAQMQAAAARAAAAAQAAAAAAAAAMwAAAHRyYW5zYWN0aW9uLnNlbmRlcl91c2VyLnRvdGFsX2Ftb3VudF9zZW50X182MDQ4MDBfXwB6/P//AAACANj7//8AAAEDEAAAAEQAAAAEAAAAAAAAADIAAAB0cmFuc2FjdGlvbi5zZW5kZXJfdXNlci50b3RhbF9hbW91bnRfc2VudF9fODY0MDBfXwAA0vz//wAAAgAw/P//AAABAxAAAABQAAAABAAAAAAAAAA/AAAAdHJhbnNhY3Rpb24uc2VuZGVyX3VzZXIuZmlyc3RfdHJhbnNhY3Rpb25fc2VudF90aW1lc3RhbXBfX2FsbF9fADb9//8AAAIAlPz//wAAAQMQAAAARAAAAAQAAAAAAAAAMgAAAHRyYW5zYWN0aW9uLnNlbmRlcl91c2VyLnA5OV9hbW91bnRfc2VudF9fMjU5MjAwMF9fAACO/f//AAACAOz8//8AAAEDEAAAAEQAAAAEAAAAAAAAADIAAAB0cmFuc2FjdGlvbi5zZW5kZXJfdXNlci5wOTVfYW1vdW50X3NlbnRfXzI1OTIwMDBfXwAA5v3//wAAAgBE/f//AAABAxAAAABEAAAABAAAAAAAAAAyAAAAdHJhbnNhY3Rpb24uc2VuZGVyX3VzZXIucDkwX2Ftb3VudF9zZW50X18yNTkyMDAwX18AAD7+//8AAAIAnP3//wAAAQMQAAAARAAAAAQAAAAAAAAAMgAAAHRyYW5zYWN0aW9uLnNlbmRlcl91c2VyLnA3NV9hbW91bnRfc2VudF9fMjU5MjAwMF9fAACW/v//AAACAPT9//8AAAEDEAAAAEQAAAAEAAAAAAAAADIAAAB0cmFuc2FjdGlvbi5zZW5kZXJfdXNlci5wNTBfYW1vdW50X3NlbnRfXzI1OTIwMDBfXwAA7v7//wAAAgBM/v//AAABAxAAAABIAAAABAAAAAAAAAA1AAAAdHJhbnNhY3Rpb24uc2VuZGVyX3VzZXIudG90YWxfYW1vdW50X2RlbHRhX18yNTkyMDAwX18AAABK////AAACAKj+//8AAAEDEAAAAEgAAAAEAAAAAAAAADQAAAB0cmFuc2FjdGlvbi5zZW5kZXJfdXNlci50b3RhbF9hbW91bnRfZGVsdGFfXzYwNDgwMF9fAAAAAKb///8AAAIABP///wAAAQMQAAAATAAAAAQAAAAAAAAAMwAAAHRyYW5zYWN0aW9uLnNlbmRlcl91c2VyLnRvdGFsX2Ftb3VudF9kZWx0YV9fODY0MDBfXwAAAAYACAAGAAYAAAAAAAIAZP///wAAAQIQAAAANAAAAAQAAAAAAAAAIwAAAHRyYW5zYWN0aW9uLnNlbmRlcl91c2VyLmFnZV9zZWNvbmRzAFz///8AAAABQAAAALD///8AAAEGEAAAAEAAAAAEAAAAAAAAACgAAAB0cmFuc2FjdGlvbi5zZW5kZXJfdXNlci5pc19lYXJseV9hZG9wdGVyAAAAAAQABAAEAAAAEAAUAAgABgAHAAwAAAAQABAAAAAAAAECEAAAADQAAAAEAAAAAAAAABoAAAB0cmFuc2FjdGlvbi5zZW5kZXJfdXNlci5pZAAACAAMAAgABwAIAAAAAAAAAUAAAADoDQAAQVJST1cx"""

# Decode base64
bs = base64.b64decode(b64_data)

# Read with polars
df = pl.read_ipc(bs)

print("Original DataFrame:")
print(f"Shape: {df.shape}")
print(f"Columns: {len(df.columns)}")
print("\nSchema:")
for col in df.columns:
    print(f"  {col}: {df[col].dtype}")

# Convert to PyArrow table
table = df.to_arrow()

# Convert LargeList to regular List (parquet_wasm compatible)
new_fields = []
new_columns = []

for i, field in enumerate(table.schema):
    col = table.column(i)

    # Check if it's a LargeList type
    if pa.types.is_large_list(field.type):
        print(f"\nConverting LargeList to List: {field.name}")

        # Get the value type from LargeList
        value_type = field.type.value_type

        # Create new List type with same value type
        new_type = pa.list_(value_type)

        # Convert the data
        # Cast LargeList to List
        new_col = col.cast(new_type)

        new_fields.append(pa.field(field.name, new_type))
        new_columns.append(new_col)
    else:
        new_fields.append(field)
        new_columns.append(col)

# Create new table with converted types
new_schema = pa.schema(new_fields)
new_table = pa.table(new_columns, schema=new_schema)

print("\n" + "="*80)
print("Converted Schema (parquet_wasm compatible):")
print("="*80)
for field in new_table.schema:
    print(f"  {field.name}: {field.type}")

# Write to Parquet
output_file = "data.parquet"
pq.write_table(new_table, output_file, compression='snappy')

print(f"\n✓ Wrote Parquet file: {output_file}")
print(f"  Size: {len(open(output_file, 'rb').read()) / 1024:.2f} KB")

# Also write Arrow IPC with regular Lists (not LargeLists)
output_ipc = "data_fixed.arrow"
with open(output_ipc, 'wb') as f:
    with pa.ipc.new_stream(f, new_table.schema) as writer:
        writer.write_table(new_table)

print(f"\n✓ Wrote Arrow IPC file: {output_ipc}")
print(f"  Size: {len(open(output_ipc, 'rb').read()) / 1024:.2f} KB")

# Verify we can read it back
print("\n" + "="*80)
print("Verification:")
print("="*80)

# Read back with polars
df_parquet = pl.read_parquet(output_file)
print(f"✓ Polars can read Parquet: {df_parquet.shape}")

df_arrow = pl.read_ipc(output_ipc)
print(f"✓ Polars can read Arrow IPC: {df_arrow.shape}")

print("\n✓ Files are ready for parquet_wasm!")
