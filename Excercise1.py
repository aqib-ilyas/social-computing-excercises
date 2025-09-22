import sqlite3
import pandas as pd

# Excercise 1.1

# Load the SQLite database
db_path = "database.sqlite"
conn = sqlite3.connect(db_path)

# Get all table names
tables_query = "SELECT name FROM sqlite_master WHERE type='table';"
tables = pd.read_sql_query(tables_query, conn)
tables_list = tables['name'].tolist()

# Inspect each table
for table in tables_list:

    if table == "sqlite_sequence":  # skip internal table
        continue
    
    print(f"\nTable: {table}")
    
    # Get column information
    columns = pd.read_sql_query(f"PRAGMA table_info({table});", conn)
    print("Columns:")
    print(columns[['name', 'type']])
    
    # Get row count
    row_count = pd.read_sql_query(f"SELECT COUNT(*) as row_count FROM {table};", conn).iloc[0, 0]
    print(f"Number of rows: {row_count}")

# Excercise 1.2

# Users who posted
posted_users = pd.read_sql_query("SELECT DISTINCT user_id FROM posts", conn)

# Users who commented
commented_users = pd.read_sql_query("SELECT DISTINCT user_id FROM comments", conn)

# Users who reacted
reacted_users = pd.read_sql_query("SELECT DISTINCT user_id FROM reactions", conn)

# All users
all_users = pd.read_sql_query("SELECT id FROM users", conn)

# Combine non-lurkers
non_lurkers = pd.concat([posted_users, commented_users, reacted_users]).drop_duplicates()

# Lurkers = all - non-lurkers
lurkers = all_users[~all_users['id'].isin(non_lurkers['user_id'])]
lurker_count = len(lurkers)

print("Number of lurkers:", lurker_count)

# Excercise 1.3

query = """
SELECT u.id AS user_id, u.username,
       COUNT(DISTINCT r.id) AS total_reactions,
       COUNT(DISTINCT c.id) AS total_comments,
       (COUNT(DISTINCT r.id) + COUNT(DISTINCT c.id)) AS engagement_score
FROM users u
LEFT JOIN posts p ON u.id = p.user_id
LEFT JOIN reactions r ON p.id = r.post_id
LEFT JOIN comments c ON p.id = c.post_id
GROUP BY u.id, u.username
ORDER BY engagement_score DESC
LIMIT 5;
"""
top_influencers = pd.read_sql_query(query, conn)
print(top_influencers)

# Excercise 1.4

query = """
SELECT user_id, content, COUNT(*) as repeat_count, 'post' as source
FROM posts
GROUP BY user_id, content
HAVING COUNT(*) >= 3

UNION ALL

SELECT user_id, content, COUNT(*) as repeat_count, 'comment' as source
FROM comments
GROUP BY user_id, content
HAVING COUNT(*) >= 3;
"""
spammers = pd.read_sql_query(query, conn)
print(spammers)
