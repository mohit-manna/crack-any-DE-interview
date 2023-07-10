# Kloud9 

## L1 Interview Questions

Questions asked in L1 Interview : 

## L2 (Client Round) Interview Questions

As informed by recruiter Nike is the Client. So we will explore Nike Interview Quesions. 

### Practice Questions
These questions are from vaarious sites posted by anonymous users.

1. about past experience and skills in data engineering

2. Why do you think you are capable of this position?

3. Basic concept of hadoop, pyspark, sql, python, kafka, scripting and cic cd pipelines, datawarehouseing concepts

4. pyspark scenarios. And coding

5. Can we have bucketing for external tables,
Can dictionary support duplicate values,
Reading a parquet file to spark DF,
Bucketing working & different types of joins in hive (bucket join , map join , functionality behind them)

### Actual Questions

1. What is your current project?

2. Airflow. How do you put schedule in airflow?

`schedule_interval` property is there which we can set at DAG level. 

3. What all operators have you used? How do you put tasks in sequence? 

Way 1: `first_task >> second_task >> [third_task, fourth_task]`

Way 2: 

```
first_task.set_downstream(second_task)
third_task.set_upstream(second_task)
```

4. No, What if there are two dags and if one task in one dag completes then we need to trigger task in second dag?

`TriggerDagRunOperator` is there in Airflow. We can use this to trigger tasks in other dags.

5. What is the technical complexity have you handled in your project?

6. How do you decide partitioning while reading from JDBC?

7. Write a code to read from JDBC?

8. Python code to calculate factorial of a number

9. Pyspark Code to find total salary an employee got in a year?

10. SQL Code to give deptwise salary give per year?

11. Write same code using subqueries. 

12. Who does all the work in Spark?

13. Code to show spark connectivity with Hive.

14. What file formats have you worked with?

15. Where does parquet stores its schema?

16. Can we read just one part file in Parquet file format?

17. How can we add columns into it? How can we add column in spark? How can we fill it with default value?

17. Where does Avro stores its schema?

18. Writea a Code to read Nested Json in Spark. How to access elements?

