# Hard Questions

## 1. Median Employee Salary

[LeetCode Problem: Median Employee Salary](https://leetcode.com/problems/median-employee-salary/description/)

**Problem:**  
Find the median salary for each company.

**Approach:**  
- For each company, assign a row number to each employee based on their salary (using `ROW_NUMBER()`).
- Calculate the total number of employees in each company (using `COUNT()`).
- The median salary is the one where the row number is between `(total_count / 2)` and `(total_count / 2) + 1`.
- Filter the Salary between these min and max counts. 

[See implementation in `median-salary.py`](../SQL/hard/median-salary/median-salary.py)

## 2. Find Cumulative Salary of an Employee
[LeetCode Problem: Find Cumulative Salary of an Employee](https://leetcode.com/problems/find-cumulative-salary-of-an-employee/description/)

**Problem:**
Get the cumulative sum of an employee's salary over a period of 3 months but exclude the most recent month.
The result should be displayed by 'Id' ascending, and then by 'Month' descending.

**Approach:**  
- row_number() to filter last month 
- sum() over() to calculate cumulative salary

[See implementation in `find-cumulative-salary-of-an-employee.py`](../SQL/hard/find-cumulative-salary-of-an-employee/find-cumulative-salary-of-an-employee.py)

## 3. Find Median Given Frequency of Numbers
[LeetCode Problem: Find Median Given Frequency of Numbers](https://leetcode.com/problems/find-median-given-frequency-of-numbers/description/)

**Problem:**
Given a list of numbers and their frequencies, find the median.
The numbers are 0, 0, 0, 0, 0, 0, 0, 1, 2, 2, 2, 3, so the median is (0 + 0) / 2 = 0

**Approach:**
- Take two datasets: one for total_freq/2 and total_freq/2 + 1.
- Use a window function to calculate the cumulative frequency.
- Find the rows where the cumulative frequency matches either of the t/2 or t/2 + 1 and do average of the numbers in those rows.

[See implementation in `find-median-given-frequency-of-numbers.py`](../SQL/hard/find-median-given-frequency-of-numbers/find-median-given-frequency-of-numbers.py)

## 4. Average Salary by Department vs Company
[LeetCode Problem: Average Salary by Department vs Company](https://leetcode.com/problems/average-salary-by-department-vs-company/description/)

**Problem:**
Given two tables `Salary` and `Employee`,
Calculate the average salary for each department and compare it with the average salary of the company.

**Approach:**
- Calculate the average salary for each department using `GROUP BY`.
- Calculate the overall average salary for the company.
- Compare the department average with the company average using `CASE WHEN`.

[See implementation in `avg-salary-dept-vs-company.py`](../SQL/hard/avg-salary-dept-vs-company/avg-salary-dept-vs-company.py)