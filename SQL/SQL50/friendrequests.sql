/*Table: RequestAccepted

+----------------+---------+
| Column Name    | Type    |
+----------------+---------+
| requester_id   | int     |
| accepter_id    | int     |
| accept_date    | date    |
+----------------+---------+
(requester_id, accepter_id) is the primary key (combination of columns with unique values) for this table.
This table contains the ID of the user who sent the request, the ID of the user who received the request, and the date when the request was accepted.
 
Write a solution to find the people who have the most friends and the most friends number.
The test cases are generated so that only one person has the most friends.
The result format is in the following example.
 
Example 1:

Input: 
RequestAccepted table:
+--------------+-------------+-------------+
| requester_id | accepter_id | accept_date |
+--------------+-------------+-------------+
| 1            | 2           | 2016/06/03  |
| 1            | 3           | 2016/06/08  |
| 2            | 3           | 2016/06/08  |
| 3            | 4           | 2016/06/09  |
+--------------+-------------+-------------+
Output: 
+----+-----+
| id | num |
+----+-----+
| 3  | 3   |
+----+-----+
Explanation: 
The person with id 3 is a friend of people 1, 2, and 4, so he has three friends in total, which is the most number than any others.
 
Follow up: In the real world, multiple people could have the same most number of friends. Could you find all these people in this case?

*/
Most Friends Query - Big Data Optimized
Code 
Why This Solution is Optimized for Big Data:
Performance Analysis:
Naive Approach (SLOW):

sql
-- TWO separate aggregations = TWO scans
SELECT requester_id AS id, COUNT(*) AS num
FROM RequestAccepted
GROUP BY requester_id

UNION ALL

SELECT accepter_id AS id, COUNT(*) AS num  
FROM RequestAccepted
GROUP BY accepter_id
Scans RequestAccepted twice
Two separate GROUP BY operations
Then needs another aggregation to sum them up
Optimized Approach (FAST):

sql
-- ONE scan with UNION ALL, then ONE aggregation
UNION ALL first → Single GROUP BY
Scans RequestAccepted once
Single GROUP BY operation
2x faster on large datasets
Key Big Data Optimizations:
1. UNION ALL Before Aggregation (Critical!)
sql
-- GOOD: Combine first, aggregate once
SELECT requester_id AS id FROM RequestAccepted
UNION ALL
SELECT accepter_id AS id FROM RequestAccepted
-- Then GROUP BY once

-- BAD: Aggregate twice, then combine
SELECT id, SUM(num) FROM (
    SELECT requester_id AS id, COUNT(*) AS num FROM RequestAccepted GROUP BY requester_id
    UNION ALL
    SELECT accepter_id AS id, COUNT(*) AS num FROM RequestAccepted GROUP BY accepter_id
) GROUP BY id
2. Avoid Subquery in WHERE Clause
sql
-- SLOWER: Scans friend_counts twice
WHERE num = (SELECT MAX(num) FROM friend_counts)

-- FASTER: Window function in single pass
RANK() OVER (ORDER BY COUNT(*) DESC)
3. Use LIMIT for Single Result
sql
-- Most efficient when only one winner
ORDER BY num DESC
LIMIT 1
-- No need for MAX subquery or window function
Performance Comparison:
Approach	Table Scans	Aggregations	Best For
Aggregate separately then combine	2	3	❌ Never use
UNION ALL then aggregate	1	1	✅ Always best
Window function	1	1	✅ When handling ties
ORDER BY + LIMIT	1	1	✅ Single winner only
Scaling Considerations:
For 100M friend relationships:

sql
-- Add partitioning hints for distributed systems
WITH all_relationships AS (
    SELECT requester_id AS id FROM RequestAccepted
    UNION ALL
    SELECT accepter_id AS id FROM RequestAccepted
)
SELECT 
    id, 
    COUNT(*) AS num,
    RANK() OVER (ORDER BY COUNT(*) DESC) AS rnk
FROM all_relationships
GROUP BY id
HAVING COUNT(*) >= 1000  -- Optional: Filter low-count users early
QUALIFY rnk = 1;  -- Some databases support QUALIFY for window filters
Memory Efficiency:
100M relationships:

Naive approach: 100M rows scanned twice = 200M row operations
Optimized approach: 100M rows → UNION ALL → 200M intermediate rows → GROUP BY once
Result: Similar intermediate size, but only one aggregation pass
Distribution Strategy (Spark/Hive):
sql
-- For distributed systems, this query:
-- 1. Distributes UNION ALL across nodes (no shuffle yet)
-- 2. Shuffles once for GROUP BY (hash partitioning on id)
-- 3. Small final dataset for ranking

-- Optimal because:
-- - Single shuffle operation (GROUP BY)
-- - No multiple scans
-- - Embarrassingly parallel UNION ALL
Index Recommendations (Traditional Databases):
sql
-- These indexes help (but limited benefit vs. full table scan)
CREATE INDEX idx_requester ON RequestAccepted(requester_id);
CREATE INDEX idx_accepter ON RequestAccepted(accepter_id);

-- For this specific query, full table scan is often faster
-- because we're reading all rows anyway
Real-world Optimization Tips:
If relationships are very skewed (some users have millions of friends):
sql
   -- Consider filtering outliers or sampling
   WHERE id NOT IN (SELECT id FROM power_users)
If you need top-K instead of just top-1:
sql
   -- Much more efficient than multiple MAX queries
   ORDER BY num DESC
   LIMIT 10
For incremental updates (streaming scenario):
sql
   -- Maintain a materialized view of friend counts
   -- Update incrementally as new relationships accepted
Why UNION ALL vs UNION?
sql
-- UNION ALL: No deduplication (FAST)
-- Perfect here because requester/accepter are different columns
-- No duplicates possible between the two sets

-- UNION: Removes duplicates (SLOW)
-- Adds unnecessary DISTINCT operation
-- Never needed for this problem
Bottom line: UNION ALL before GROUP BY is the key optimization. This reduces the problem to a single scan and single aggregation, which is optimal for big data processing.