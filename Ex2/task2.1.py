import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta

# Connect to database
db_path = "../database.sqlite"
conn = sqlite3.connect(db_path)

# Exercise 2.1 - Growth Analysis

# Get growth data over time
# Aggregate all activity (users, posts, comments, reactions) by month
query = """
SELECT 
    DATE(created_at, 'start of month') as month,
    COUNT(*) as count
FROM (
    SELECT created_at FROM users
    UNION ALL
    SELECT created_at FROM posts
    UNION ALL
    SELECT created_at FROM comments
) 
GROUP BY month
ORDER BY month;
"""

growth_data = pd.read_sql_query(query, conn)
growth_data['month'] = pd.to_datetime(growth_data['month'])

# Calculate cumulative growth (total activity over time)
growth_data['cumulative_activity'] = growth_data['count'].cumsum()

# Fit a trend line (linear regression)
# Convert dates to numeric values for regression
growth_data['month_numeric'] = (growth_data['month'] - growth_data['month'].min()).dt.days

X = growth_data['month_numeric'].values
y = growth_data['cumulative_activity'].values

# Fit polynomial (degree 2 for better fit if growth is accelerating)
coefficients = np.polyfit(X, y, 2)
trend_function = np.poly1d(coefficients)

# Current situation
current_date = growth_data['month'].max()
current_activity = growth_data['cumulative_activity'].iloc[-1]
current_servers = 16

print(f"Current date (last data point): {current_date.strftime('%Y-%m-%d')}")
print(f"Current cumulative activity: {current_activity:,.0f}")
print(f"Current servers: {current_servers}")
print(f"Activity per server: {current_activity / current_servers:,.0f}")

# Project 3 years into the future
days_in_3_years = 3 * 365
future_date = current_date + timedelta(days=days_in_3_years)
future_month_numeric = X[-1] + days_in_3_years

# Predict future activity
predicted_future_activity = trend_function(future_month_numeric)

print(f"\nProjected date (3 years ahead): {future_date.strftime('%Y-%m-%d')}")
print(f"Projected cumulative activity: {predicted_future_activity:,.0f}")

# Calculate growth factor
growth_factor = predicted_future_activity / current_activity

print(f"Growth factor (3 years): {growth_factor:.2f}x")

# Calculate servers needed
servers_needed_base = current_servers * growth_factor
servers_with_redundancy = servers_needed_base * 1.20  # Add 20% redundancy

print(f"\nServers needed (base): {servers_needed_base:.1f}")
print(f"Servers needed (with 20% redundancy): {servers_with_redundancy:.1f}")
print(f"Servers to rent (rounded up): {int(np.ceil(servers_with_redundancy))}")
print(f"Additional servers needed: {int(np.ceil(servers_with_redundancy)) - current_servers}")

# Create visualization
fig, ax = plt.subplots(figsize=(12, 7))

# Plot historical data
ax.plot(growth_data['month'], growth_data['cumulative_activity'], 
        'o-', label='Historical Activity', linewidth=2, markersize=4, color='#2E86AB')

# Plot trend line
trend_dates = pd.date_range(growth_data['month'].min(), future_date, periods=100)
trend_numeric = [(d - growth_data['month'].min()).days for d in trend_dates]
trend_values = trend_function(trend_numeric)
ax.plot(trend_dates, trend_values, '--', label='Trend Line', 
        linewidth=2, color='#A23B72', alpha=0.8)

# Mark current point
ax.plot(current_date, current_activity, 'o', markersize=12, 
        color='#F18F01', label='Current (16 servers)', zorder=5)

# Mark projected point
ax.plot(future_date, predicted_future_activity, 's', markersize=12, 
        color='#C73E1D', label=f'Projected (3 years)', zorder=5)

# Add vertical line for current date
ax.axvline(current_date, color='gray', linestyle=':', alpha=0.5)

# Formatting
ax.set_xlabel('Date', fontsize=12, fontweight='bold')
ax.set_ylabel('Cumulative Activity', fontsize=12, fontweight='bold')
ax.set_title('Social Media Platform Growth Trend & Server Capacity Projection', 
             fontsize=14, fontweight='bold', pad=20)
ax.legend(loc='upper left', fontsize=10)
ax.grid(True, alpha=0.3)

# Format y-axis with comma separators
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))

# Add annotation for projection
ax.annotate(f'{int(np.ceil(servers_with_redundancy))} servers needed\n(with 20% redundancy)',
            xy=(future_date, predicted_future_activity),
            xytext=(20, -40), textcoords='offset points',
            bbox=dict(boxstyle='round,pad=0.5', facecolor='yellow', alpha=0.7),
            arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0', lw=2),
            fontsize=10, fontweight='bold')

plt.tight_layout()
plt.savefig('server_growth_projection.png', dpi=300, bbox_inches='tight')
plt.show()

print(f"Current servers: {current_servers}")
print(f"Servers needed in 3 years: {int(np.ceil(servers_with_redundancy))}")
print(f"Additional servers to rent: {int(np.ceil(servers_with_redundancy)) - current_servers}")

conn.close()