import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os

# Exercise 2.2 - Virality Analysis

# Connect to database - adjust path as needed
db_path = "../database.sqlite"
conn = sqlite3.connect(db_path)

# Average engagement per post
avg_engagement_query = """
SELECT 
    AVG(reaction_count) as avg_reactions,
    AVG(comment_count) as avg_comments,
    AVG(reaction_count + comment_count) as avg_total_engagement
FROM (
    SELECT 
        p.id,
        COUNT(DISTINCT r.id) as reaction_count,
        COUNT(DISTINCT c.id) as comment_count
    FROM posts p
    LEFT JOIN reactions r ON p.id = r.post_id
    LEFT JOIN comments c ON p.id = c.post_id
    GROUP BY p.id
);
"""
avg_stats = pd.read_sql_query(avg_engagement_query, conn)
print(f"Average reactions per post: {avg_stats['avg_reactions'].iloc[0]:.2f}")
print(f"Average comments per post: {avg_stats['avg_comments'].iloc[0]:.2f}")
print(f"Average total engagement per post: {avg_stats['avg_total_engagement'].iloc[0]:.2f}")

# Get follower count for each user
follower_query = """
SELECT followed_id as user_id, COUNT(*) as follower_count
FROM follows
GROUP BY followed_id;
"""
followers = pd.read_sql_query(follower_query, conn)

# Calculate virality metrics for all posts
virality_query = """
SELECT 
    p.id as post_id,
    p.user_id,
    u.username,
    p.content,
    p.created_at,
    COUNT(DISTINCT r.id) as reaction_count,
    COUNT(DISTINCT c.id) as comment_count,
    (COUNT(DISTINCT r.id) + COUNT(DISTINCT c.id)) as total_engagement,
    COUNT(DISTINCT c.user_id) as unique_commenters
FROM posts p
JOIN users u ON p.user_id = u.id
LEFT JOIN reactions r ON p.id = r.post_id
LEFT JOIN comments c ON p.id = c.post_id
GROUP BY p.id, p.user_id, u.username, p.content, p.created_at;
"""
posts_engagement = pd.read_sql_query(virality_query, conn)

print(f"\nTotal posts analyzed: {len(posts_engagement)}")

# Merge with follower data
posts_engagement = posts_engagement.merge(followers, on='user_id', how='left')
posts_engagement['follower_count'] = posts_engagement['follower_count'].fillna(0)

# Calculate virality score components
# 1. Engagement rate = total_engagement / (follower_count + 1) 
posts_engagement['engagement_rate'] = posts_engagement['total_engagement'] / (posts_engagement['follower_count'] + 1)

# 2. Comment ratio = comments / (reactions + 1)
posts_engagement['comment_ratio'] = posts_engagement['comment_count'] / (posts_engagement['reaction_count'] + 1)

# Normalize metrics to 0-1 scale for fair comparison
def normalize(series):
    min_val = series.min()
    max_val = series.max()
    if max_val - min_val == 0:
        return pd.Series([0] * len(series))
    return (series - min_val) / (max_val - min_val)

posts_engagement['norm_engagement'] = normalize(posts_engagement['total_engagement'])
posts_engagement['norm_engagement_rate'] = normalize(posts_engagement['engagement_rate'])
posts_engagement['norm_comment_ratio'] = normalize(posts_engagement['comment_ratio'])

# Calculate composite virality score
# Weighted combination: 40% absolute engagement, 40% engagement rate, 20% comment ratio
posts_engagement['virality_score'] = (
    0.40 * posts_engagement['norm_engagement'] +
    0.40 * posts_engagement['norm_engagement_rate'] +
    0.20 * posts_engagement['norm_comment_ratio']
)

# Sort by virality score and get top 3
viral_posts = posts_engagement.sort_values('virality_score', ascending=False).head(3)

for i, (idx, post) in enumerate(viral_posts.iterrows()):
    print(f"RANK #{i + 1}")
    print(f"Post ID: {int(post['post_id'])}")
    print(f"Author: {post['username']} (User ID: {int(post['user_id'])})")
    print(f"Created: {post['created_at']}")
    print(f"\nContent Preview:")
    content_preview = post['content'][:200] + ('...' if len(post['content']) > 200 else '')
    print(f"  \"{content_preview}\"")
    print(f"\nENGAGEMENT METRICS")
    print(f"Total Reactions: {int(post['reaction_count'])}")
    print(f"Total Comments: {int(post['comment_count'])}")
    print(f"Total Engagement: {int(post['total_engagement'])}")
    print(f"Unique Commenters: {int(post['unique_commenters'])}")
    print(f"Author's Followers: {int(post['follower_count'])}")
    print(f"\nVIRALITY METRICS")
    print(f"Engagement Rate: {post['engagement_rate']:.2f} (engagement per follower)")
    print(f"Comment Ratio: {post['comment_ratio']:.3f}")
    print(f"VIRALITY SCORE: {post['virality_score']:.4f}")
    
    # Show why it's viral
    avg_engagement = avg_stats['avg_total_engagement'].iloc[0]
    multiplier = post['total_engagement'] / avg_engagement
    print(f"\nWhy it's viral: {multiplier:.1f}x more engagement than average post")

# Create visualization
fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle('Viral Posts Analysis - Top 3 Most Viral Posts', 
             fontsize=16, fontweight='bold', y=0.995)

# Plot 1: Virality Score Comparison (Top 20)
ax1 = axes[0, 0]
top_20 = posts_engagement.nlargest(20, 'virality_score')
colors = ['#C73E1D' if i < 3 else '#2E86AB' for i in range(len(top_20))]
bars = ax1.barh(range(len(top_20)), top_20['virality_score'], color=colors)
ax1.set_yticks(range(len(top_20)))
ax1.set_yticklabels([f"Post {int(id)}" for id in top_20['post_id']], fontsize=8)
ax1.set_xlabel('Virality Score', fontweight='bold')
ax1.set_title('Top 20 Posts by Virality Score', fontweight='bold')
ax1.invert_yaxis()
ax1.grid(axis='x', alpha=0.3)

# Plot 2: Engagement Rate vs Total Engagement (scatter)
ax2 = axes[0, 1]
ax2.scatter(posts_engagement['total_engagement'], 
           posts_engagement['engagement_rate'],
           alpha=0.5, s=30, color='#2E86AB', label='All Posts')
ax2.scatter(viral_posts['total_engagement'], 
           viral_posts['engagement_rate'],
           s=200, color='#C73E1D', edgecolors='black', linewidths=2,
           label='Top 3 Viral', zorder=5)
for i, (idx, post) in enumerate(viral_posts.iterrows()):
    ax2.annotate(f"#{i+1}", 
                xy=(post['total_engagement'], post['engagement_rate']),
                fontsize=10, fontweight='bold', ha='center', va='center',
                color='white')
ax2.set_xlabel('Total Engagement', fontweight='bold')
ax2.set_ylabel('Engagement Rate (engagement/follower)', fontweight='bold')
ax2.set_title('Engagement Rate vs Total Engagement', fontweight='bold')
ax2.legend()
ax2.grid(alpha=0.3)

# Plot 3: Component Breakdown for Top 3
ax3 = axes[1, 0]
components = ['Absolute\nEngagement\n(40%)', 'Engagement\nRate\n(40%)', 'Comment\nRatio\n(20%)']
weights = [0.40, 0.40, 0.20]
x = np.arange(len(components))
width = 0.25

colors_bars = ['#C73E1D', '#F18F01', '#2E86AB']
for i, (idx, post) in enumerate(viral_posts.iterrows()):
    values = [
        post['norm_engagement'] * weights[0],
        post['norm_engagement_rate'] * weights[1],
        post['norm_comment_ratio'] * weights[2]
    ]
    ax3.bar(x + i*width, values, width, label=f"Rank #{i+1}", color=colors_bars[i])

ax3.set_xlabel('Virality Components', fontweight='bold')
ax3.set_ylabel('Weighted Score Contribution', fontweight='bold')
ax3.set_title('Virality Score Breakdown (Top 3)', fontweight='bold')
ax3.set_xticks(x + width)
ax3.set_xticklabels(components, fontsize=9)
ax3.legend()
ax3.grid(axis='y', alpha=0.3)

# Plot 4: Reactions vs Comments for Top 3
ax4 = axes[1, 1]
rank_labels = [f"Rank #{i+1}\nPost {int(post['post_id'])}" 
               for i, (idx, post) in enumerate(viral_posts.iterrows())]
reactions = viral_posts['reaction_count'].values
comments = viral_posts['comment_count'].values

x_pos = np.arange(len(rank_labels))
width = 0.35

ax4.bar(x_pos - width/2, reactions, width, label='Reactions', color='#F18F01')
ax4.bar(x_pos + width/2, comments, width, label='Comments', color='#A23B72')

ax4.set_xlabel('Post Rank', fontweight='bold')
ax4.set_ylabel('Count', fontweight='bold')
ax4.set_title('Reactions vs Comments (Top 3 Viral Posts)', fontweight='bold')
ax4.set_xticks(x_pos)
ax4.set_xticklabels(rank_labels, fontsize=9)
ax4.legend()
ax4.grid(axis='y', alpha=0.3)

plt.tight_layout()
plt.savefig('viral_posts_analysis.png', dpi=300, bbox_inches='tight')
print("\n Visualization saved as 'viral_posts_analysis.png'")
plt.show()
conn.close()