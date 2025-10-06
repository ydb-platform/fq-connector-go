#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Massively-parallel reading from MDB PostgreSQL on YDB Analytics Prestable
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import re
from matplotlib.ticker import MultipleLocator

# Read the CSV data
df = pd.read_csv("./ydb_20251006.csv")
print("Raw data:")
print(df)

# Function to convert duration in format "Xm Ys" to seconds
def duration_to_seconds(duration_str):
    # Extract minutes and seconds using regex
    match = re.match(r'(\d+)m\s+(\d+)s', duration_str)
    if match:
        minutes, seconds = map(int, match.groups())
        return minutes * 60 + seconds
    return 0

# Convert duration strings to seconds
df['duration_seconds'] = df['duration'].apply(duration_to_seconds)
print("\nData with duration in seconds:")
print(df)

# Define the data size constant (143.95 GiB)
DATA_SIZE_GIB = 143.95
# Convert GiB to MiB (1 GiB = 1024 MiB)
DATA_SIZE_MIB = DATA_SIZE_GIB * 1024

# Calculate throughput (MiB/s) by dividing data size by duration in seconds
df['throughput_mibs'] = DATA_SIZE_MIB / df['duration_seconds']
print("\nData with calculated throughput:")
print(df)

# Create the plot
plt.figure(figsize=(10, 6))

# Plot the data
plt.plot(df['MaxTasksPerStage'], df['throughput_mibs'], marker='o', linewidth=2, markersize=8)

# Set the title and labels
plt.title('Massively-parallel reading from MDB PostgreSQL on YDB Analytics Prestable', fontsize=14)
plt.xlabel('MaxTasksPerStage', fontsize=12)
plt.ylabel('Throughput (MiB/s)', fontsize=12)

# Set grid
plt.grid(True, linestyle='--', alpha=0.7)

# Customize x-axis to show all task values
plt.xticks(df['MaxTasksPerStage'])

# Add value annotations
for x, y in zip(df['MaxTasksPerStage'], df['throughput_mibs']):
    plt.annotate(f'{y:.2f}', (x, y), textcoords="offset points", 
                 xytext=(0, 10), ha='center')

plt.tight_layout()
plt.savefig('ydb_20251006.png')
plt.show()
