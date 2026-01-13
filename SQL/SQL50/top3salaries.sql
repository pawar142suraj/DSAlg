/*
Table: Employee

+--------------+---------+
| Column Name  | Type    |
+--------------+---------+
| id           | int     |
| name         | varchar |
| salary       | int     |
| departmentId | int     |
+--------------+---------+
id is the primary key (column with unique values) for this table.
departmentId is a foreign key (reference column) of the ID from the Department table.
Each row of this table indicates the ID, name, and salary of an employee. It also contains the ID of their department.
 

Table: Department

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| id          | int     |
| name        | varchar |
+-------------+---------+
id is the primary key (column with unique values) for this table.
Each row of this table indicates the ID of a department and its name.
 

A company's executives are interested in seeing who earns the most money in each of the company's departments. A high earner in a department is an employee who has a salary in the top three unique salaries for that department.

Write a solution to find the employees who are high earners in each of the departments.

Return the result table in any order.

The result format is in the following example.

 

Example 1:

Input: 
Employee table:
+----+-------+--------+--------------+
| id | name  | salary | departmentId |
+----+-------+--------+--------------+
| 1  | Joe   | 85000  | 1            |
| 2  | Henry | 80000  | 2            |
| 3  | Sam   | 60000  | 2            |
| 4  | Max   | 90000  | 1            |
| 5  | Janet | 69000  | 1            |
| 6  | Randy | 85000  | 1            |
| 7  | Will  | 70000  | 1            |
+----+-------+--------+--------------+
Department table:
+----+-------+
| id | name  |
+----+-------+
| 1  | IT    |
| 2  | Sales |
+----+-------+
Output: 
+------------+----------+--------+
| Department | Employee | Salary |
+------------+----------+--------+
| IT         | Max      | 90000  |
| IT         | Joe      | 85000  |
| IT         | Randy    | 85000  |
| IT         | Will     | 70000  |
| Sales      | Henry    | 80000  |
| Sales      | Sam      | 60000  |
+------------+----------+--------+
Explanation: 
In the IT department:
- Max earns the highest unique salary
- Both Randy and Joe earn the second-highest unique salary
- Will earns the third-highest unique salary

In the Sales department:
- Henry earns the highest salary
- Sam earns the second-highest salary
- There is no third-highest salary as there are only two employees
 

Constraints:

There are no employees with the exact same name, salary and department./
*/

-- Big Data Optimized Solution
-- Key: Use DENSE_RANK to handle ties and get top 3 UNIQUE salaries
-- Single scan with window function, then join Department only once

-- Solution 1: Window function with DENSE_RANK (Most Efficient)
WITH ranked_salaries AS (
    SELECT 
        e.id,
        e.name,
        e.salary,
        e.departmentId,
        -- DENSE_RANK: Same salary gets same rank, next different salary increments by 1
        -- This handles "top 3 UNIQUE salaries" requirement
        DENSE_RANK() OVER (
            PARTITION BY e.departmentId 
            ORDER BY e.salary DESC
        ) AS salary_rank
    FROM Employee e
)
SELECT 
    d.name AS Department,
    r.name AS Employee,
    r.salary AS Salary
FROM ranked_salaries r
JOIN Department d ON r.departmentId = d.id
WHERE r.salary_rank <= 3
ORDER BY d.name, r.salary DESC;


-- Solution 2: Using ROW_NUMBER (if you want distinct employees, not unique salaries)
-- NOTE: This is NOT correct for this problem, but shown for comparison
WITH ranked_employees AS (
    SELECT 
        e.id,
        e.name,
        e.salary,
        e.departmentId,
        -- ROW_NUMBER: Each employee gets unique rank even with same salary
        ROW_NUMBER() OVER (
            PARTITION BY e.departmentId 
            ORDER BY e.salary DESC
        ) AS employee_rank
    FROM Employee e
)
SELECT 
    d.name AS Department,
    r.name AS Employee,
    r.salary AS Salary
FROM ranked_employees r
JOIN Department d ON r.departmentId = d.id
WHERE r.employee_rank <= 3;  -- Gets top 3 employees, not top 3 unique salaries


-- Solution 3: Self-join approach (Traditional but SLOWER for big data)
-- Counts how many distinct higher salaries exist
SELECT 
    d.name AS Department,
    e1.name AS Employee,
    e1.salary AS Salary
FROM Employee e1
JOIN Department d ON e1.departmentId = d.id
WHERE (
    -- Count distinct salaries higher than current employee's salary
    SELECT COUNT(DISTINCT e2.salary)
    FROM Employee e2
    WHERE e2.departmentId = e1.departmentId
      AND e2.salary > e1.salary
) < 3
ORDER BY d.name, e1.salary DESC;


-- Solution 4: For very large datasets (Optimized with early filtering)
-- Filter departments and employees before ranking if you have criteria
WITH ranked_salaries AS (
    SELECT 
        e.id,
        e.name,
        e.salary,
        e.departmentId,
        DENSE_RANK() OVER (
            PARTITION BY e.departmentId 
            ORDER BY e.salary DESC
        ) AS salary_rank
    FROM Employee e
    -- Add filters here if needed (e.g., active employees only)
    -- WHERE e.status = 'ACTIVE'
)
SELECT 
    d.name AS Department,
    r.name AS Employee,
    r.salary AS Salary
FROM ranked_salaries r
JOIN Department d ON r.departmentId = d.id
WHERE r.salary_rank <= 3
ORDER BY Department, Salary DESC, Employee;


-- Solution 5: Alternative with RANK (handles ties differently)
WITH ranked_salaries AS (
    SELECT 
        e.id,
        e.name,
        e.salary,
        e.departmentId,
        -- RANK: Same salary gets same rank, but skips next ranks
        -- e.g., if two employees at rank 1, next is rank 3 (not 2)
        RANK() OVER (
            PARTITION BY e.departmentId 
            ORDER BY e.salary DESC
        ) AS salary_rank
    FROM Employee e
)
SELECT 
    d.name AS Department,
    r.name AS Employee,
    r.salary AS Salary
FROM ranked_salaries r
JOIN Department d ON r.departmentId = d.id
WHERE r.salary_rank <= 3;