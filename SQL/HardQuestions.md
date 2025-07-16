# Hard Questions

## 1. Median Employee Salary

[LeetCode Problem: Median Employee Salary](https://leetcode.com/problems/median-employee-salary/description/)

**Problem:**  
Find the median salary for each company.

**Approach:**  
1. For each company, assign a row number to each employee based on their salary (using `ROW_NUMBER()`).
2. Calculate the total number of employees in each company (using `COUNT()`).
3. The median salary is the one where the row number is between `(total_count / 2)` and `(total_count / 2) + 1`.
4. Filter the Salary between these min and max counts. 

[See implementation in `median-salary.py`](../SQL/hard/median-salary.py)

