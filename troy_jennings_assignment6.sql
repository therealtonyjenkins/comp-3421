USE empDB;

-- Q1: Find all information about managers who are 25 years old or younger and live in California (‘CA’).
SELECT e.*
FROM Employee e
INNER JOIN Manages m ON m.eid = e.eid
WHERE e.age <= 25
	AND e.residenceState = 'CA';

-- Q2: Find the name, salary, age, and residence state of all 20- year-old or younger managers who live 
--     in Indiana (‘IN’).
SELECT e.name, e.salary, e.age, e.residenceState
FROM Employee e
INNER JOIN Manages m ON m.eid = e.eid
WHERE e.age <= 20
	AND e.residenceState = 'IN';

-- Q3: Find the names and salary of 25-year-old employees who work for departments located on the fourth 
--     floor in Alaska (‘AK’).
SELECT e.name, e.salary
FROM Employee e
INNER JOIN WorksFor w ON e.eid = w.eid
INNER JOIN Department d ON w.did = d.did
WHERE e.age = 25
	AND d.stateLocated = 'AK'
    AND d.floor = 4;

-- Q4: Find the name, salary, and EID of 49-year-old employees who work for a department located in 
--     Alaska (‘AK’) but live in California (‘CA’).
SELECT e.name, e.salary, e.eid
FROM Employee e
INNER JOIN WorksFor w ON e.eid = w.eid
INNER JOIN Department d ON w.did = d.did
WHERE e.age = 49
	AND d.stateLocated = 'AK'
    AND e.residenceState = 'CA';

-- Q5: Find the total number of employees.
SELECT COUNT(*) FROM Employee;

-- Q6: Find the number of employees who are managers. 
SELECT COUNT(*)
FROM Employee e
WHERE e.eid IN (SELECT m.eid FROM Manages m);

-- Q7: Find the number of employees who are not managers.
SELECT COUNT(*)
FROM Employee e
WHERE e.eid NOT IN (
	SELECT m.eid FROM Manages m
);

-- Q8: Find the (eid,number) pair for employees who are managing two or more departments where "number" 
--     is the number of departments they are managing.
SELECT e.eid, COUNT(m.did) AS dept_count
FROM Employee e
INNER JOIN Manages m ON e.eid = m.eid
GROUP BY 1
HAVING dept_count >= 2
ORDER BY 2 DESC;

-- Q9: Present the (name1, salary1, name2, salary2), where salary1 is the salary of the employee with name1 
--     and salary2 corresponds with name2, of all employee pairs where both are living in California (‘CA’), 
--     one is a 24-year-old manager, the other (who can be any age) is not a manager, and the manager earns 
--     more than three times the other employee.

WITH Manager AS (
	SELECT e.eid, e.name, e.salary
    FROM Employee e
    INNER JOIN Manages m ON e.eid = m.eid
    WHERE e.age = 24
		AND e.residenceState = 'CA'
),
NonManager AS (
	SELECT e.eid, e.name, e.salary
    FROM Employee e
    WHERE e.eid NOT IN (
		SELECT m.eid FROM Manages m
	)
		AND e.residenceState = 'CA'
)
SELECT m.name, m.salary, n.name, n.salary
FROM Manager m, NonManager n
WHERE m.salary > n.salary * 3;

-- Q10: For each department in Alaska ('AK') that has 25 or more employees working for it and a supply 
--      budget < $7,000, present the did, budget, and number of employees that work in that department.
SELECT d.did, d.supplyBudget, COUNT(w.eid) AS empl_count
FROM Department d
INNER JOIN WorksFor w ON d.did = w.did
WHERE d.supplyBudget < 7000
	AND d.stateLocated = 'AK'
GROUP BY 1, 2
HAVING empl_count >= 25
ORDER BY 2 DESC;

-- Q11: For each state, present the salary of the average 20-year- old manager (i.e., average salary of mangers 
--      who are 20 years old) who lives in that state and the number of such managers. Note: Your results can 
--      omit states that do not have any 20-year- old managers living in them.
SELECT e.residenceState, AVG(e.salary), COUNT(m.eid)
FROM Employee e
INNER JOIN Manages m ON e.eid = m.eid
GROUP BY 1;