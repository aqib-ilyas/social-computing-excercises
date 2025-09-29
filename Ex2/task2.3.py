import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import os

# Exercise 2.3 - Content Lifecycle Analysis

# Connect to database
db_path = "../database.sqlite"
conn = sqlite3.connect(db_path)

# Step 1: Get all engagement events with their timestamps based on comments and posts

engagement_query = """
SELECT 
    p.id as post_id,
    p.created_at as post_created,
    c.created_at as engagement_time,
    'comment' as engagement_type
FROM posts p
JOIN comments c ON p.id = c.post_id
WHERE c.created_at IS NOT NULL;
"""

# Get all engagement data
engagements = pd.read_sql_query(engagement_query, conn)

print(f"\n--- Analysis Dataset ---")
print(f"Total engagement events analyzed: {len(engagements)}")
print(f"Unique posts with engagement: {engagements['post_id'].nunique()}")

# Convert to datetime
engagements['post_created'] = pd.to_datetime(engagements['post_created'])
engagements['engagement_time'] = pd.to_datetime(engagements['engagement_time'])

# Calculate time difference in seconds
engagements['time_diff_seconds'] = (engagements['engagement_time'] - engagements['post_created']).dt.total_seconds()

# Remove negative time differences (data quality issue)
original_count = len(engagements)
engagements = engagements[engagements['time_diff_seconds'] >= 0]
removed = original_count - len(engagements)
if removed > 0:
    print(f"Removed {removed} engagements with negative time differences (data quality issue)")

# Calculate first and last engagement for each post
post_lifecycle = engagements.groupby('post_id').agg({
    'time_diff_seconds': ['min', 'max', 'count'],
    'post_created': 'first'
}).reset_index()

post_lifecycle.columns = ['post_id', 'first_engagement_seconds', 'last_engagement_seconds', 
                          'engagement_count', 'post_created']

# Convert to more readable units
post_lifecycle['first_engagement_hours'] = post_lifecycle['first_engagement_seconds'] / 3600
post_lifecycle['last_engagement_hours'] = post_lifecycle['last_engagement_seconds'] / 3600
post_lifecycle['first_engagement_days'] = post_lifecycle['first_engagement_seconds'] / 86400
post_lifecycle['last_engagement_days'] = post_lifecycle['last_engagement_seconds'] / 86400

# Calculate lifecycle duration (time between first and last engagement)
post_lifecycle['lifecycle_duration_seconds'] = post_lifecycle['last_engagement_seconds'] - post_lifecycle['first_engagement_seconds']
post_lifecycle['lifecycle_duration_hours'] = post_lifecycle['lifecycle_duration_seconds'] / 3600
post_lifecycle['lifecycle_duration_days'] = post_lifecycle['lifecycle_duration_seconds'] / 86400

# Calculate averages
avg_first_engagement_seconds = post_lifecycle['first_engagement_seconds'].mean()
avg_last_engagement_seconds = post_lifecycle['last_engagement_seconds'].mean()
avg_lifecycle_duration_seconds = post_lifecycle['lifecycle_duration_seconds'].mean()

avg_first_engagement_hours = avg_first_engagement_seconds / 3600
avg_last_engagement_hours = avg_last_engagement_seconds / 3600
avg_first_engagement_days = avg_first_engagement_seconds / 86400
avg_last_engagement_days = avg_last_engagement_seconds / 86400
avg_lifecycle_duration_days = avg_lifecycle_duration_seconds / 86400

# Median values (often more representative than mean)
median_first_engagement_seconds = post_lifecycle['first_engagement_seconds'].median()
median_last_engagement_seconds = post_lifecycle['last_engagement_seconds'].median()
median_lifecycle_duration_seconds = post_lifecycle['lifecycle_duration_seconds'].median()

median_first_engagement_hours = median_first_engagement_seconds / 3600
median_last_engagement_hours = median_last_engagement_seconds / 3600
median_first_engagement_days = median_first_engagement_seconds / 86400
median_last_engagement_days = median_last_engagement_seconds / 86400
median_lifecycle_duration_days = median_lifecycle_duration_seconds / 86400

print("RESULTS - CONTENT LIFECYCLE METRICS")

print("\nTIME TO FIRST ENGAGEMENT")
print(f"Average: {avg_first_engagement_hours:.2f} hours ({avg_first_engagement_days:.2f} days)")
print(f"Median:  {median_first_engagement_hours:.2f} hours ({median_first_engagement_days:.2f} days)")

print("\nTIME TO LAST ENGAGEMENT")
print(f"Average: {avg_last_engagement_hours:.2f} hours ({avg_last_engagement_days:.2f} days)")
print(f"Median:  {median_last_engagement_hours:.2f} hours ({median_last_engagement_days:.2f} days)")

print("\nCONTENT LIFECYCLE DURATION")
print(f"(Time between first and last engagement)")
print(f"Average: {avg_lifecycle_duration_days:.2f} days")
print(f"Median:  {median_lifecycle_duration_days:.2f} days")

print(f"Total posts analyzed: {len(post_lifecycle)}")
print(f"Posts with only 1 engagement: {(post_lifecycle['engagement_count'] == 1).sum()}")
print(f"Posts with 2+ engagements: {(post_lifecycle['engagement_count'] >= 2).sum()}")

# Distribution statistics
print("\nDISTRIBUTION STATISTICS")
print(f"\nFirst Engagement Time:")
print(f"  Min:  {post_lifecycle['first_engagement_hours'].min():.2f} hours")
print(f"  25%:  {post_lifecycle['first_engagement_hours'].quantile(0.25):.2f} hours")
print(f"  50%:  {post_lifecycle['first_engagement_hours'].quantile(0.50):.2f} hours")
print(f"  75%:  {post_lifecycle['first_engagement_hours'].quantile(0.75):.2f} hours")
print(f"  Max:  {post_lifecycle['first_engagement_hours'].max():.2f} hours")

print(f"\nLast Engagement Time:")
print(f"  Min:  {post_lifecycle['last_engagement_hours'].min():.2f} hours")
print(f"  25%:  {post_lifecycle['last_engagement_hours'].quantile(0.25):.2f} hours")
print(f"  50%:  {post_lifecycle['last_engagement_hours'].quantile(0.50):.2f} hours")
print(f"  75%:  {post_lifecycle['last_engagement_hours'].quantile(0.75):.2f} hours")
print(f"  Max:  {post_lifecycle['last_engagement_hours'].max():.2f} hours")

# Create visualizations
fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle('Content Lifecycle Analysis', fontsize=16, fontweight='bold', y=0.995)

# Plot 1: Distribution of time to first engagement (histogram)
ax1 = axes[0, 0]
# Cap at 95th percentile for better visualization
first_eng_cap = post_lifecycle['first_engagement_hours'].quantile(0.95)
first_eng_filtered = post_lifecycle[post_lifecycle['first_engagement_hours'] <= first_eng_cap]['first_engagement_hours']
ax1.hist(first_eng_filtered, bins=50, color='#2E86AB', alpha=0.7, edgecolor='black')
ax1.axvline(avg_first_engagement_hours, color='#C73E1D', linestyle='--', linewidth=2, label=f'Mean: {avg_first_engagement_hours:.1f}h')
ax1.axvline(median_first_engagement_hours, color='#F18F01', linestyle='--', linewidth=2, label=f'Median: {median_first_engagement_hours:.1f}h')
ax1.set_xlabel('Hours', fontweight='bold')
ax1.set_ylabel('Number of Posts', fontweight='bold')
ax1.set_title('Time to First Engagement (95th percentile)', fontweight='bold')
ax1.legend()
ax1.grid(alpha=0.3)

# Plot 2: Distribution of time to last engagement (histogram)
ax2 = axes[0, 1]
last_eng_cap = post_lifecycle['last_engagement_hours'].quantile(0.95)
last_eng_filtered = post_lifecycle[post_lifecycle['last_engagement_hours'] <= last_eng_cap]['last_engagement_hours']
ax2.hist(last_eng_filtered, bins=50, color='#A23B72', alpha=0.7, edgecolor='black')
ax2.axvline(avg_last_engagement_hours, color='#C73E1D', linestyle='--', linewidth=2, label=f'Mean: {avg_last_engagement_hours:.1f}h')
ax2.axvline(median_last_engagement_hours, color='#F18F01', linestyle='--', linewidth=2, label=f'Median: {median_last_engagement_hours:.1f}h')
ax2.set_xlabel('Hours', fontweight='bold')
ax2.set_ylabel('Number of Posts', fontweight='bold')
ax2.set_title('Time to Last Engagement (95th percentile)', fontweight='bold')
ax2.legend()
ax2.grid(alpha=0.3)

# Plot 3: Box plot comparison
ax3 = axes[1, 0]
data_to_plot = [
    post_lifecycle['first_engagement_hours'],
    post_lifecycle['last_engagement_hours']
]
bp = ax3.boxplot(data_to_plot, labels=['First Engagement', 'Last Engagement'],
                 patch_artist=True, showmeans=True)
for patch, color in zip(bp['boxes'], ['#2E86AB', '#A23B72']):
    patch.set_facecolor(color)
    patch.set_alpha(0.7)
ax3.set_ylabel('Hours', fontweight='bold')
ax3.set_title('Engagement Timing Distribution', fontweight='bold')
ax3.grid(alpha=0.3, axis='y')

# Plot 4: Scatter plot - first vs last engagement time
ax4 = axes[1, 1]
# Sample for better visualization if too many points
sample_size = min(1000, len(post_lifecycle))
sample = post_lifecycle.sample(n=sample_size, random_state=42)
ax4.scatter(sample['first_engagement_hours'], sample['last_engagement_hours'], 
           alpha=0.5, s=30, color='#2E86AB')
# Add diagonal line (where first = last)
max_val = max(sample['first_engagement_hours'].max(), sample['last_engagement_hours'].max())
ax4.plot([0, max_val], [0, max_val], 'r--', linewidth=2, label='First = Last', alpha=0.5)
ax4.set_xlabel('Time to First Engagement (hours)', fontweight='bold')
ax4.set_ylabel('Time to Last Engagement (hours)', fontweight='bold')
ax4.set_title('First vs Last Engagement Time', fontweight='bold')
ax4.legend()
ax4.grid(alpha=0.3)

plt.tight_layout()
plt.savefig('content_lifecycle_analysis.png', dpi=300, bbox_inches='tight')
print("\n Visualization saved as 'content_lifecycle_analysis.png'")
plt.show()
conn.close()