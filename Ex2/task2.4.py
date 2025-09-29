import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os

# Exercise 2.4 - User Connections Analysis

# Connect to database
db_path = "../database.sqlite"
conn = sqlite3.connect(db_path)

# Step 1: Get all comment engagements (User A comments on User B's post)
comments_engagement_query = """
SELECT 
    c.user_id as engager_id,
    p.user_id as content_owner_id,
    u1.username as engager_username,
    u2.username as content_owner_username,
    COUNT(*) as comment_count
FROM comments c
JOIN posts p ON c.post_id = p.id
JOIN users u1 ON c.user_id = u1.id
JOIN users u2 ON p.user_id = u2.id
WHERE c.user_id != p.user_id  -- Exclude self-engagement
GROUP BY c.user_id, p.user_id, u1.username, u2.username;
"""

comments_engagement = pd.read_sql_query(comments_engagement_query, conn)
print(f"\nComment engagement patterns found: {len(comments_engagement)}")

# Step 2: Get all reaction engagements (User A reacts to User B's post)
reactions_engagement_query = """
SELECT 
    r.user_id as engager_id,
    p.user_id as content_owner_id,
    u1.username as engager_username,
    u2.username as content_owner_username,
    COUNT(*) as reaction_count
FROM reactions r
JOIN posts p ON r.post_id = p.id
JOIN users u1 ON r.user_id = u1.id
JOIN users u2 ON p.user_id = u2.id
WHERE r.user_id != p.user_id  -- Exclude self-reactions
GROUP BY r.user_id, p.user_id, u1.username, u2.username;
"""

reactions_engagement = pd.read_sql_query(reactions_engagement_query, conn)
print(f"Reaction engagement patterns found: {len(reactions_engagement)}")

# Step 3: Combine comments and reactions with weights
# Merge the two dataframes
all_engagement = comments_engagement.merge(
    reactions_engagement, 
    on=['engager_id', 'content_owner_id', 'engager_username', 'content_owner_username'],
    how='outer'
)

# Fill NaN values with 0
all_engagement['comment_count'] = all_engagement['comment_count'].fillna(0).astype(int)
all_engagement['reaction_count'] = all_engagement['reaction_count'].fillna(0).astype(int)

# Calculate weighted engagement score (comments worth 2x reactions)
all_engagement['engagement_score'] = (
    all_engagement['comment_count'] * 2 + 
    all_engagement['reaction_count'] * 1
)

print(f"\nTotal unique directional engagements: {len(all_engagement)}")

# Step 4: Create user pairs and calculate mutual engagement
# For each pair, we need to sum both directions: A->B and B->A

# Create a normalized pair identifier (always smaller ID first)
def make_pair_id(row):
    id1, id2 = row['engager_id'], row['content_owner_id']
    return tuple(sorted([id1, id2]))

all_engagement['pair_id'] = all_engagement.apply(make_pair_id, axis=1)

# Group by pair and sum engagement from both directions
pair_engagement = all_engagement.groupby('pair_id').agg({
    'engagement_score': 'sum',
    'comment_count': 'sum',
    'reaction_count': 'sum'
}).reset_index()

# Get usernames for each pair
pair_details = []
for pair_id in pair_engagement['pair_id']:
    user1_id, user2_id = pair_id
    
    # Get engagements in both directions
    dir1 = all_engagement[
        (all_engagement['engager_id'] == user1_id) & 
        (all_engagement['content_owner_id'] == user2_id)
    ]
    dir2 = all_engagement[
        (all_engagement['engager_id'] == user2_id) & 
        (all_engagement['content_owner_id'] == user1_id)
    ]
    
    # Get usernames
    if len(dir1) > 0:
        user1_name = dir1.iloc[0]['engager_username']
        user2_name = dir1.iloc[0]['content_owner_username']
    elif len(dir2) > 0:
        user1_name = dir2.iloc[0]['content_owner_username']
        user2_name = dir2.iloc[0]['engager_username']
    else:
        # Fallback: query database
        user1_name = pd.read_sql_query(
            f"SELECT username FROM users WHERE id = {user1_id}", conn
        ).iloc[0, 0]
        user2_name = pd.read_sql_query(
            f"SELECT username FROM users WHERE id = {user2_id}", conn
        ).iloc[0, 0]
    
    # Calculate engagement in each direction
    eng1to2_comments = dir1['comment_count'].sum() if len(dir1) > 0 else 0
    eng1to2_reactions = dir1['reaction_count'].sum() if len(dir1) > 0 else 0
    eng2to1_comments = dir2['comment_count'].sum() if len(dir2) > 0 else 0
    eng2to1_reactions = dir2['reaction_count'].sum() if len(dir2) > 0 else 0
    
    pair_details.append({
        'user1_id': user1_id,
        'user2_id': user2_id,
        'user1_name': user1_name,
        'user2_name': user2_name,
        'user1_to_user2_comments': eng1to2_comments,
        'user1_to_user2_reactions': eng1to2_reactions,
        'user2_to_user1_comments': eng2to1_comments,
        'user2_to_user1_reactions': eng2to1_reactions,
        'user1_to_user2_score': eng1to2_comments * 2 + eng1to2_reactions,
        'user2_to_user1_score': eng2to1_comments * 2 + eng2to1_reactions
    })

pair_details_df = pd.DataFrame(pair_details)

# Merge with engagement scores
final_pairs = pair_engagement.merge(
    pair_details_df,
    left_on='pair_id',
    right_on=pair_details_df.apply(lambda x: tuple(sorted([x['user1_id'], x['user2_id']])), axis=1)
)

# Sort by mutual engagement score
final_pairs = final_pairs.sort_values('engagement_score', ascending=False)

# Get top 3
top_3_pairs = final_pairs.head(3)

print(f"\nTotal user pairs with mutual engagement: {len(final_pairs)}")
print(f"Average mutual engagement score: {final_pairs['engagement_score'].mean():.2f}")
print(f"Median mutual engagement score: {final_pairs['engagement_score'].median():.2f}")

print("\n" + "="*80)
print("TOP 3 USER PAIRS WITH HIGHEST MUTUAL ENGAGEMENT")
print("="*80)

for i, (idx, pair) in enumerate(top_3_pairs.iterrows()):
    print(f"\n{'='*80}")
    print(f"RANK #{i + 1}")
    print(f"{'='*80}")
    print(f"User Pair: {pair['user1_name']} (ID: {pair['user1_id']}) ↔ {pair['user2_name']} (ID: {pair['user2_id']})")
    print(f"\nMUTUAL ENGAGEMENT SCORE: {int(pair['engagement_score'])}")
    print(f"Total Comments: {int(pair['comment_count'])}")
    print(f"Total Reactions: {int(pair['reaction_count'])}")
    
    print(f"\n--- Directional Breakdown ---")
    print(f"{pair['user1_name']} → {pair['user2_name']}:")
    print(f"  Comments: {int(pair['user1_to_user2_comments'])}, Reactions: {int(pair['user1_to_user2_reactions'])}, Score: {int(pair['user1_to_user2_score'])}")
    
    print(f"{pair['user2_name']} → {pair['user1_name']}:")
    print(f"  Comments: {int(pair['user2_to_user1_comments'])}, Reactions: {int(pair['user2_to_user1_reactions'])}, Score: {int(pair['user2_to_user1_score'])}")
    
    # Calculate balance
    balance = abs(pair['user1_to_user2_score'] - pair['user2_to_user1_score']) / pair['engagement_score']
    if balance < 0.2:
        balance_desc = "Highly Balanced"
    elif balance < 0.4:
        balance_desc = "Moderately Balanced"
    else:
        balance_desc = "Unbalanced"
    
    print(f"\nEngagement Balance: {balance_desc} ({(1-balance)*100:.1f}% reciprocal)")

# Create visualizations
fig = plt.figure(figsize=(16, 10))
gs = fig.add_gridspec(3, 3, hspace=0.3, wspace=0.3)

fig.suptitle('User Connections Analysis - Top 3 Most Connected Pairs', 
             fontsize=16, fontweight='bold')

# Plot 1: Bar chart - Top 10 pairs by engagement score
ax1 = fig.add_subplot(gs[0, :])
top_10 = final_pairs.head(10)
pair_labels = [f"{row['user1_name'][:10]}\n↔\n{row['user2_name'][:10]}" 
               for _, row in top_10.iterrows()]
colors = ['#C73E1D' if i < 3 else '#2E86AB' for i in range(len(top_10))]
bars = ax1.bar(range(len(top_10)), top_10['engagement_score'], color=colors, edgecolor='black')
ax1.set_xticks(range(len(top_10)))
ax1.set_xticklabels(pair_labels, fontsize=9, rotation=0)
ax1.set_ylabel('Mutual Engagement Score', fontweight='bold', fontsize=11)
ax1.set_title('Top 10 User Pairs by Mutual Engagement', fontweight='bold', fontsize=12)
ax1.grid(axis='y', alpha=0.3)

# Add value labels on bars
for i, (bar, val) in enumerate(zip(bars, top_10['engagement_score'])):
    ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
             f'{int(val)}', ha='center', va='bottom', fontweight='bold', fontsize=9)

# Plot 2, 3, 4: Directional engagement for top 3
for i, (idx, pair) in enumerate(top_3_pairs.iterrows()):
    ax = fig.add_subplot(gs[1, i])
    
    categories = [f"{pair['user1_name'][:8]}\n→\n{pair['user2_name'][:8]}", 
                  f"{pair['user2_name'][:8]}\n→\n{pair['user1_name'][:8]}"]
    
    comments = [pair['user1_to_user2_comments'], pair['user2_to_user1_comments']]
    reactions = [pair['user1_to_user2_reactions'], pair['user2_to_user1_reactions']]
    
    x = np.arange(len(categories))
    width = 0.35
    
    bars1 = ax.bar(x - width/2, comments, width, label='Comments (2pts)', color='#A23B72', edgecolor='black')
    bars2 = ax.bar(x + width/2, reactions, width, label='Reactions (1pt)', color='#F18F01', edgecolor='black')
    
    ax.set_ylabel('Count', fontweight='bold', fontsize=10)
    ax.set_title(f'Rank #{i+1}: {pair["user1_name"][:10]} ↔ {pair["user2_name"][:10]}', 
                fontweight='bold', fontsize=11)
    ax.set_xticks(x)
    ax.set_xticklabels(categories, fontsize=8)
    ax.legend(fontsize=8)
    ax.grid(axis='y', alpha=0.3)

# Plot 5: Distribution of mutual engagement scores
ax5 = fig.add_subplot(gs[2, 0])
ax5.hist(final_pairs['engagement_score'], bins=50, color='#2E86AB', alpha=0.7, edgecolor='black')
ax5.axvline(final_pairs['engagement_score'].mean(), color='#C73E1D', 
           linestyle='--', linewidth=2, label=f'Mean: {final_pairs["engagement_score"].mean():.1f}')
ax5.axvline(final_pairs['engagement_score'].median(), color='#F18F01', 
           linestyle='--', linewidth=2, label=f'Median: {final_pairs["engagement_score"].median():.1f}')
ax5.set_xlabel('Mutual Engagement Score', fontweight='bold', fontsize=10)
ax5.set_ylabel('Number of User Pairs', fontweight='bold', fontsize=10)
ax5.set_title('Distribution of Mutual Engagement', fontweight='bold', fontsize=11)
ax5.legend(fontsize=8)
ax5.grid(alpha=0.3)

# Plot 6: Comments vs Reactions contribution for top 3
ax6 = fig.add_subplot(gs[2, 1])
pair_names = [f"{row['user1_name'][:8]}\n↔\n{row['user2_name'][:8]}" 
              for _, row in top_3_pairs.iterrows()]
comment_scores = [(row['comment_count'] * 2) for _, row in top_3_pairs.iterrows()]
reaction_scores = [row['reaction_count'] for _, row in top_3_pairs.iterrows()]

x_pos = np.arange(len(pair_names))
ax6.bar(x_pos, comment_scores, label='Comments (2pts each)', color='#A23B72', edgecolor='black')
ax6.bar(x_pos, reaction_scores, bottom=comment_scores, label='Reactions (1pt each)', 
       color='#F18F01', edgecolor='black')

ax6.set_ylabel('Score Contribution', fontweight='bold', fontsize=10)
ax6.set_title('Score Composition (Top 3)', fontweight='bold', fontsize=11)
ax6.set_xticks(x_pos)
ax6.set_xticklabels(pair_names, fontsize=8)
ax6.legend(fontsize=8)
ax6.grid(axis='y', alpha=0.3)

# Plot 7: Engagement balance for top 10
ax7 = fig.add_subplot(gs[2, 2])
top_10_balance = []
for _, row in top_10.iterrows():
    balance = min(row['user1_to_user2_score'], row['user2_to_user1_score']) / max(row['user1_to_user2_score'], row['user2_to_user1_score']) if max(row['user1_to_user2_score'], row['user2_to_user1_score']) > 0 else 0
    top_10_balance.append(balance * 100)

pair_labels_short = [f"{row['user1_name'][:6]}↔{row['user2_name'][:6]}" 
                     for _, row in top_10.iterrows()]
colors_balance = ['#C73E1D' if i < 3 else '#2E86AB' for i in range(len(top_10))]
ax7.barh(range(len(top_10)), top_10_balance, color=colors_balance, edgecolor='black')
ax7.set_yticks(range(len(top_10)))
ax7.set_yticklabels(pair_labels_short, fontsize=8)
ax7.set_xlabel('Reciprocity %', fontweight='bold', fontsize=10)
ax7.set_title('Engagement Reciprocity (Top 10)', fontweight='bold', fontsize=11)
ax7.invert_yaxis()
ax7.grid(axis='x', alpha=0.3)
ax7.axvline(80, color='green', linestyle='--', alpha=0.5, label='80% threshold')
ax7.legend(fontsize=8)

plt.savefig('user_connections_analysis.png', dpi=300, bbox_inches='tight')
print("\nVisualization saved as 'user_connections_analysis.png'")
plt.show()

print(f"\nEngagement Score Distribution:")
print(f"  Minimum: {final_pairs['engagement_score'].min():.0f}")
print(f"  25th percentile: {final_pairs['engagement_score'].quantile(0.25):.0f}")
print(f"  Median: {final_pairs['engagement_score'].median():.0f}")
print(f"  75th percentile: {final_pairs['engagement_score'].quantile(0.75):.0f}")
print(f"  Maximum: {final_pairs['engagement_score'].max():.0f}")

# Compare top 3 to average
print(f"\nTop 3 pairs vs platform average:")
avg_score = final_pairs['engagement_score'].mean()
for i, (idx, pair) in enumerate(top_3_pairs.iterrows()):
    multiplier = pair['engagement_score'] / avg_score
    print(f"  Rank #{i+1}: {multiplier:.1f}x more engagement than average pair")

conn.close()
