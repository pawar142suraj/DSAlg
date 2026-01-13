/*
Over budget on a project is defined when the salaries, prorated to the day, exceed the budget of the project.

For example, if Alice and Bob both combined income make 200K and work on a project of a budget of 50K that takes half a year, then the project is over budget given 0.5 * 200K = 100K > 50K.

Write a query to forecast the budget for all projects and return a label of "overbudget" if it is over budget and "within budget" otherwise.

Note: Assume that employees only work on one project at a time.

Example:

Input:

employees table

Column	Type
id	INTEGER
first_name	VARCHAR
last_name	VARCHAR
salary	INTEGER
department_id	INTEGER
projects table

Column	Type
id	INTEGER
title	VARCHAR
state_date	DATETIME
end_date	DATETIME
budget	INTEGER
departments table

Column	Type
id	INTEGER
name	VARCHAR
employee_projects table

Column	Type
project_id	INTEGER
employee_id	INTEGER
Output:

Column	Type
title	VARCHAR
project_forecast	VARCHAR

*/
/* Write your T-SQL query statement below */
  
  
with project_cost as (
    select 
    ep.project_id,
    sum(e.salary * datediff(day,p.state_date,p.end_date)/365.0) as total_cost
    from employee_projects ep
    join employees e on ep.employee_id = e.id
    join projects p on ep.project_id = p.id
    group by ep.project_id
)
select 
p.title,    
case when pc.total_cost > p.budget then 'overbudget' else 'within budget' end as project_forecast
from projects p
left join project_cost pc on p.id = pc.project_id