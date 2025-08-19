#!/usr/bin/env python3
"""
Throughput Analysis Script

This script analyzes throughput data from parallel read operations with different
numbers of connectors and tasks, creating visualizations of the results.
Designed for presentation at IT conferences.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os
import sys
import traceback
from matplotlib import rcParams, cycler
from matplotlib.patches import Rectangle

# Set up the matplotlib parameters for a professional look
plt.style.use('seaborn-v0_8-whitegrid')
rcParams['font.family'] = 'DejaVu Sans'
rcParams['font.size'] = 14
rcParams['axes.titlesize'] = 22
rcParams['axes.labelsize'] = 18
rcParams['xtick.labelsize'] = 18  # Increased for visibility in large rooms
rcParams['ytick.labelsize'] = 18  # Increased for visibility in large rooms
rcParams['legend.fontsize'] = 16
rcParams['figure.titlesize'] = 24

def main():
    try:
        print("\n" + "="*80)
        print("PARALLEL READ PERFORMANCE ANALYSIS".center(80))
        print("="*80 + "\n")
        
        # Get the directory of the script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Path to the CSV file
        csv_path = os.path.join(script_dir, 'yq_dev_20250810.csv')
        print(f"Looking for CSV file at: {csv_path}")
        
        # Check if the file exists
        if not os.path.exists(csv_path):
            print(f"Error: CSV file not found at {csv_path}")
            sys.exit(1)
        
        # Load the CSV file
        print("Loading data...")
        df = pd.read_csv(csv_path)
        print(f"Data loaded successfully. Found {len(df)} rows with columns: {', '.join(df.columns)}")
        
        # Display the first few rows
        print("\nSample data:")
        print(df.head().to_string())
        
        # Check for missing values
        missing = df.isna().sum()
        print("\nMissing values:")
        for col, count in missing.items():
            print(f"  {col}: {count}")
        
        # Remove rows with missing Time values
        df_clean = df.dropna(subset=['Time'])
        print(f"\nRemoved {len(df) - len(df_clean)} rows with missing Time values.")
        
        # Calculate throughput (MB/s) by dividing 8.54 GB by Time
        GB_CONSTANT = 8.54
        # Calculate throughput (MB/s) normalized by the number of threads (Tasks)
        df_clean['Throughput_MBps'] = ((GB_CONSTANT * 1024) / df_clean['Time']) / df_clean['Tasks']
        
        print(f"Processed {len(df_clean)} rows of data.")
        print(f"Throughput range: {df_clean['Throughput_MBps'].min():.2f} - {df_clean['Throughput_MBps'].max():.2f} MB/s")
        
        # Create the visualization
        print("\nCreating throughput plot...")
        fig = create_throughput_plot(df_clean)
        
        # Save the plot in high resolution for presentation with transparent background
        output_path = os.path.join(script_dir, 'throughput_analysis.png')
        print(f"Saving high-resolution PNG to: {output_path}")
        fig.savefig(output_path, dpi=600, bbox_inches='tight', format='png', transparent=True)
        print(f"✓ High-resolution plot with transparent background saved successfully.")
        
        # Print key insights for the presentation
        print_insights(df_clean)
        
        print("\n" + "="*80)
        print("ANALYSIS COMPLETE".center(80))
        print("="*80)
        print(f"\nVisualization saved in: {script_dir}")
        print(f"File created: {os.path.basename(output_path)} - Throughput analysis")
        
    except Exception as e:
        print("\nERROR: An unexpected error occurred:")
        print(f"{type(e).__name__}: {e}")
        print("\nDetailed traceback:")
        traceback.print_exc()
        sys.exit(1)

def create_throughput_plot(df):
    """Create a presentation-quality visualization of throughput vs tasks grouped by number of connectors."""
    # Set up the plot with a 4:3 aspect ratio for presentations
    fig, ax = plt.subplots(figsize=(12, 9), dpi=100)  # 4:3 aspect ratio
    
    # Use a professional color palette
    # Using a custom palette that's more vibrant and distinguishable in presentations
    colors = sns.color_palette("mako_r", len(sorted(df['Replicas'].unique())))
    
    # Set transparent background
    ax.set_facecolor('none')
    fig.patch.set_facecolor('none')
    
    # Add a subtle grid
    ax.grid(True, linestyle='--', alpha=0.7, color='#cccccc')
    
    # Create the scatter plot with connecting lines
    unique_replicas = sorted(df['Replicas'].unique(), reverse=True)  # Sort in descending order
    color_map = dict(zip(unique_replicas, colors))
    
    # Plot each connector group
    for replica in unique_replicas:  # Now in descending order (8, 4, 2, 1)
        subset = df[df['Replicas'] == replica]
        # Sort by Tasks to ensure lines connect points in the right order
        subset = subset.sort_values('Tasks')
        
        # Plot line first (behind points)
        line = ax.plot(
            subset['Tasks'],
            subset['Throughput_MBps'],
            color=color_map[replica],
            alpha=0.8,
            linestyle='-',
            linewidth=3,
            zorder=1
        )
        
        # Then plot points on top
        scatter = ax.scatter(
            subset['Tasks'],
            subset['Throughput_MBps'],
            s=180,  # Larger point size for visibility
            color=color_map[replica],
            edgecolor='white',
            linewidth=2,
            alpha=1.0,
            label=f"{replica} instance{'s' if replica > 1 else ''}",  # "1 instance", "2 instances", etc.
            zorder=2
        )
        
        # No data labels as per request
    
    # Maximum throughput point is not annotated as per request
    
    # Set x-axis to log scale for better visualization of the range
    ax.set_xscale('log', base=2)
    ax.set_xticks(sorted(df['Tasks'].unique()))
    ax.set_xticklabels(sorted(df['Tasks'].unique()))
    
    # Add labels and title with more professional styling
    ax.set_xlabel('YQ reading threads', fontweight='bold', labelpad=15)  # More space between ticks and label
    ax.set_ylabel('Throughput per thread (MB/s)', fontweight='bold', labelpad=15)  # More space between ticks and label
    ax.set_title('Massively parallel reading from external YDB',
                 fontweight='bold', pad=20)
    
    # No data source label as per request
    
    # Customize the legend with transparent background
    legend = ax.legend(
        title='Connector',
        title_fontsize=16,
        frameon=True,
        fancybox=True,
        framealpha=0.5,  # Semi-transparent background for better readability
        shadow=False,  # No shadow as per request
        borderpad=1,
        loc='upper left'
    )
    
    # Set the title font weight manually if supported
    try:
        legend.get_title().set_fontweight('bold')
    except:
        pass
    
    # Add a box around the plot area
    for spine in ax.spines.values():
        spine.set_visible(True)
        spine.set_color('#888888')
        spine.set_linewidth(0.5)
    
    # No watermark as per request
    
    # Adjust layout
    plt.tight_layout()  # No need for extra space at the bottom now
    
    return fig


def create_performance_zones_plot(df):
    """Create a visualization with performance zones to highlight optimal configurations."""
    # Set up the plot
    fig, ax = plt.subplots(figsize=(16, 10), dpi=100)
    
    # Add a subtle background color
    ax.set_facecolor('#f8f9fa')
    fig.patch.set_facecolor('#f8f9fa')
    
    # Calculate performance thresholds
    max_throughput = df['Throughput_MBps'].max()
    high_perf_threshold = max_throughput * 0.8
    medium_perf_threshold = max_throughput * 0.5
    
    # Add performance zones
    ax.axhspan(high_perf_threshold, max_throughput * 1.1, alpha=0.2, color='green', label='High Performance Zone')
    ax.axhspan(medium_perf_threshold, high_perf_threshold, alpha=0.2, color='yellow', label='Medium Performance Zone')
    ax.axhspan(0, medium_perf_threshold, alpha=0.2, color='red', label='Low Performance Zone')
    
    # Add zone labels
    ax.text(64, max_throughput * 1.05, 'HIGH PERFORMANCE ZONE', fontsize=12, ha='right', color='darkgreen', fontweight='bold')
    ax.text(64, high_perf_threshold - 5, 'MEDIUM PERFORMANCE ZONE', fontsize=12, ha='right', color='darkgoldenrod', fontweight='bold')
    ax.text(64, medium_perf_threshold - 5, 'LOW PERFORMANCE ZONE', fontsize=12, ha='right', color='darkred', fontweight='bold')
    
    # Use a professional color palette
    unique_replicas = sorted(df['Replicas'].unique())
    colors = sns.color_palette("mako_r", len(unique_replicas))
    color_map = dict(zip(unique_replicas, colors))
    
    # Plot each connector group
    for replica in unique_replicas:
        subset = df[df['Replicas'] == replica]
        subset = subset.sort_values('Tasks')
        
        # Plot line first
        line = ax.plot(
            subset['Tasks'],
            subset['Throughput_MBps'],
            color=color_map[replica],
            alpha=0.8,
            linestyle='-',
            linewidth=3,
            zorder=1
        )
        
        # Then plot points
        scatter = ax.scatter(
            subset['Tasks'],
            subset['Throughput_MBps'],
            s=180,
            color=color_map[replica],
            edgecolor='white',
            linewidth=2,
            alpha=1.0,
            label=f"{replica} connector{'s' if replica > 1 else ''}",
            zorder=2
        )
    
    # Add a grid
    ax.grid(True, linestyle='--', alpha=0.7, color='#cccccc')
    
    # Set x-axis to log scale
    ax.set_xscale('log', base=2)
    ax.set_xticks(sorted(df['Tasks'].unique()))
    ax.set_xticklabels(sorted(df['Tasks'].unique()))
    
    # Add labels and title
    ax.set_xlabel('Number of Tasks', fontweight='bold')
    ax.set_ylabel('Throughput per thread (MB/s)', fontweight='bold')
    ax.set_title('Parallel Read Performance Analysis:\nPerformance Zones by Configuration',
                 fontweight='bold', pad=20)
    
    # Add a subtitle
    plt.figtext(0.5, 0.01, 'Data source: YQ Development Benchmark (August 2025)',
                ha='center', fontsize=12, fontstyle='italic')
    
    # Customize the legend
    legend = ax.legend(
        title='Configuration',
        title_fontsize=16,
        frameon=True,
        fancybox=True,
        framealpha=0.9,
        shadow=True,
        borderpad=1,
        loc='upper left'
    )
    
    # Set the title font weight manually if supported
    try:
        legend.get_title().set_fontweight('bold')
    except:
        pass
    
    # Add a box around the plot area
    for spine in ax.spines.values():
        spine.set_visible(True)
        spine.set_color('#888888')
        spine.set_linewidth(0.5)
    
    # Add a watermark
    fig.text(0.95, 0.05, 'FQ Connector',
             fontsize=16, color='gray', alpha=0.5,
             ha='right', va='bottom', rotation=0)
    
    # Adjust layout
    plt.tight_layout(rect=[0, 0.03, 1, 0.97])
    
    return fig


def print_insights(df):
    """Print key insights for the presentation."""
    print("\n" + "="*80)
    print("KEY INSIGHTS FOR PRESENTATION".center(80))
    print("="*80)
    
    # Calculate statistics by replica count
    stats_by_replica = df.groupby('Replicas')['Throughput_MBps'].agg(['mean', 'min', 'max'])
    print("\nPer-Thread Throughput Statistics by Number of Connectors:")
    for replica, row in stats_by_replica.iterrows():
        print(f"  {replica} connector{'s' if replica > 1 else ''}: "
              f"Avg: {row['mean']:.1f} MB/s, Min: {row['min']:.1f} MB/s, Max: {row['max']:.1f} MB/s")
    
    # Find the maximum throughput configuration
    max_throughput_idx = df['Throughput_MBps'].idxmax()
    max_config = df.loc[max_throughput_idx]
    print(f"\nOptimal Configuration:")
    print(f"  {max_config['Replicas']} connectors with {max_config['Tasks']} tasks "
          f"→ {max_config['Throughput_MBps']:.1f} MB/s per thread")
    
    # Calculate performance thresholds
    max_throughput = df['Throughput_MBps'].max()
    high_perf_threshold = max_throughput * 0.8
    medium_perf_threshold = max_throughput * 0.5
    
    # Find configurations in each performance zone
    high_perf = df[df['Throughput_MBps'] >= high_perf_threshold]
    medium_perf = df[(df['Throughput_MBps'] >= medium_perf_threshold) &
                     (df['Throughput_MBps'] < high_perf_threshold)]
    
    print("\nPer-Thread Performance Zone Analysis:")
    print(f"  High Performance Zone (>{high_perf_threshold:.1f} MB/s per thread):")
    for _, row in high_perf.iterrows():
        print(f"    - {row['Replicas']} connector{'s' if row['Replicas'] > 1 else ''} with {row['Tasks']} tasks: {row['Throughput_MBps']:.1f} MB/s per thread")
    
    print(f"\n  Medium Performance Zone ({medium_perf_threshold:.1f}-{high_perf_threshold:.1f} MB/s per thread):")
    for _, row in medium_perf.iterrows():
        print(f"    - {row['Replicas']} connector{'s' if row['Replicas'] > 1 else ''} with {row['Tasks']} tasks: {row['Throughput_MBps']:.1f} MB/s per thread")
    
    print("\nRecommendations for Production Deployments:")
    print("  1. High-throughput environments: Deploy 8 connectors with 64 tasks")
    print("  2. Balanced environments: Deploy 4 connectors with 32-64 tasks")
    print("  3. Resource-constrained environments: Deploy 2 connectors with 32 tasks")
    
    print("="*80)

if __name__ == "__main__":
    main()