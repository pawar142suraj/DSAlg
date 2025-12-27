/*


Table: Activity

+--------------+---------+
| Column Name  | Type    |
+--------------+---------+
| player_id    | int     |
| device_id    | int     |
| event_date   | date    |
| games_played | int     |
+--------------+---------+
(player_id, event_date) is the primary key (combination of columns with unique values) of this table.
This table shows the activity of players of some games.
Each row is a record of a player who logged in and played a number of games (possibly 0) before logging out on someday using some device.

Write a solution to report the fraction of players that logged in again on the day after the day they first logged in, rounded to 2 decimal places. In other words, you need to determine the number of players who logged in on the day immediately following their initial login, and divide it by the number of total players.

The result format is in the following example.

 

Example 1:

Input: 
Activity table:
+-----------+-----------+------------+--------------+
| player_id | device_id | event_date | games_played |
+-----------+-----------+------------+--------------+
| 1         | 2         | 2016-03-01 | 5            |
| 1         | 2         | 2016-03-02 | 6            |
| 2         | 3         | 2017-06-25 | 1            |
| 3         | 1         | 2016-03-02 | 0            |
| 3         | 4         | 2018-07-03 | 5            |
+-----------+-----------+------------+--------------+
Output: 
+-----------+
| fraction  |
+-----------+
| 0.33      |
+-----------+
Explanation: 
Only the player with id 1 logged back in after the first day he had logged in so the answer is 1/3 = 0.33
*/
/* Write your T-SQL query statement below */

WITH NEXT_LOGIN AS (
select
    PLAYER_ID
    ,DATEDIFF(day,event_date, (lead(event_date) over(partition by player_id order by event_date asc)))
    as DIFF
    ,ROW_NUMBER() OVER (partition BY PLAYER_ID order by event_date asc ) AS RN
    --,COUNT(1) 
from activity
)
,NO_OF_PLAYERS AS (SELECT  COUNT(DISTINCT PLAYER_ID)*1.0 AS CNT FROM ACTIVITY)
,
FINAL AS (
SELECT

    ISNULL(ROUND(SUM(1)/(SELECT * FROM NO_OF_PLAYERS ),2),0) AS FRACTION
    --CASE WHEN DIFF=1 THEN 1 ELSE 0,
    --NO_OF_PLAYERS 
FROM NEXT_LOGIN
WHERE DIFF = 1 AND RN =1
)
SELECT * FROM FINAL