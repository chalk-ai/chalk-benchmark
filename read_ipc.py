#!/usr/bin/env python3
"""
Read Arrow IPC Stream file
"""

import pyarrow as pa
import pyarrow.ipc as ipc

# Read as IPC Stream (not File format)
with open('t.ipc', 'rb') as f:
    reader = ipc.open_stream(f)

    # Read all batches into a table
    table = reader.read_all()

print(f"Rows: {table.num_rows}")
print(f"Columns: {table.num_columns}")

print("\nSchema:")
for field in table.schema:
    print(f"  {field.name}: {field.type}")

# Convert to pandas for easy viewing
df = table.to_pandas()

print("\nFirst few rows:")
print(df.head())

print("\nColumn names:")
for i, col in enumerate(df.columns):
    print(f"  {i}: {col}")
