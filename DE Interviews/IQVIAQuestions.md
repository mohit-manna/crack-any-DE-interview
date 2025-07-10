## IQVIA Snowflake developer questions
### Project related: 

1. **How did you optimize costs in your project?**
- Seeing the usage of tables and deleting and deleting tables not being used
- Changed ORC tables to Delta format
- Optimizing data pipelines 
- Monitoring cluster idle times. 
Todo: 
- Have a detailed approach and write up around this. 
- Pain points while migration and their solutions. 
2. **What catalog is used in databricks?**

Catalog helps to manage access, enforce security, and track lineage for data stored. Databricks supports Hive Metastore too but Unity Catalog is recommended. 

| Catalog | Type | Best For | Cloud Support |
| :-- | :-- | :-- | :-- |
| Unity Catalog | Databricks | Lakehouse governance and lineage | Multi-cloud (AWS, Azure, GCP) |
| Hive Metastore | Open-source | Legacy big data ecosystems | On-premise, Cloud |
| AWS Glue Data Catalog | AWS | AWS-native metadata management | AWS only |
| Apache Atlas | Open-source | Hadoop-based metadata management | On-premise, Cloud |
| Google Cloud Data Catalog | GCP | GCP-native metadata management | GCP only |
| Microsoft Purview | Azure | Azure-native metadata management | Azure only |
| Alation | Commercial | Enterprise data discovery | Multi-cloud |
| Collibra | Commercial | Enterprise data governance | Multi-cloud |
| Informatica Catalog | Commercial | Large-scale enterprise environments | Multi-cloud |
| OpenMetadata | Open-source | Modern data platforms | Multi-cloud |
| Amundsen | Open-source | Data discovery and collaboration | Multi-cloud |

Todo : 
- Giving access to a person or team in various catalogs. 
- Querying catalog using LLMs. 

3. **What Airflow Operators did you use ?**

- Bash Operator
- Python Operator
- Kubernetes Operator

Todo: 
- Creating Custom Operator in Airflow

4. **How does Vertica stores data inside it ?**

| Feature | Description |
| :-- | :-- |
| Columnar Storage | Data is stored column-by-column for efficient querying and compression. |
| Projections | Optimized physical representations of tables for specific query patterns. |
| Compression | Advanced techniques like RLE, Delta Encoding, and Dictionary Encoding. |
| Segmentation | Logical partitions of data distributed across nodes for parallel processing. |
| WOS and ROS | Temporary (WOS) and permanent (ROS) storage layers for efficient data handling. |
| Tuple Mover | Background process for moving and optimizing data between WOS and ROS. |
| Replication | Ensures fault tolerance and high availability in a cluster. |


### SQL: 

1. **write down output when you do self inner join for the following table:**
```
A,B
CQ,CZ
X,X
Y,X
X,Y
P,M
Z,L
```

[See implementation in `self-inner-join.py`](../Spark/self-inner-join.py)

2. **Department wise highest salary?**

```
with windowed as (
    select id,departmentId,salary,
           row_number() over (partition by departmentId order by salary desc) as row_num
    from employee
)
select id,departmentId,salary
from windowed
where row_num = 1
```

[See implementation in `department-top-three-salaries.py`](../Spark/department-top-three-salaries.py)

3. **What are the disadvantages of using CTEs or 'with' clause ?**

- In some database systems, CTEs are materialized, which may impact performance. PostgresSQL & SQL Server inlines it with main query, this behaviour depends on query optimizer. 
- CTEs are temporary and do not have indexes. If the CTE involves large datasets, operations like filtering, joining, or aggregating can be slower compared to using indexed tables or materialized views.
- Query Optimizer might not work: the optimizer might treat the CTE as a "black box" and fail to push down filters or optimize joins effectively.
- In a multi-user environment, heavy use of CTEs in complex queries can lead to resource contention, affecting the performance of other queries running on the same system.
- Recursive CTEs may lead to performance issues or even stack overflow errors if not carefully designed.

**When to Avoid CTEs**
- When the CTE is referenced multiple times in the same query:
    Consider using a temporary table or a subquery instead to avoid redundant computations.
- When dealing with very large datasets:
    Use indexed tables or materialized views for better performance.
- When the query optimizer struggles to optimize the CTE:
    Test alternative approaches like subqueries or derived tables.
- When the logic needs to be reused across multiple queries:
    Use a view or a stored procedure instead of a CTE.

4. **Can you optimize this query?**

```
-- Step 1: Find the maximum salary for each department
WITH max_salaries AS (
    SELECT departmentId, MAX(salary) AS max_salary
    FROM employee
    GROUP BY departmentId
)
-- Step 2: Join with the original table to get the employee details
SELECT e.id, e.departmentId, e.salary
FROM employee e
JOIN max_salaries ms
  ON e.departmentId = ms.departmentId AND e.salary = ms.max_salary;
```

- Window functions like ROW_NUMBER() can be computationally expensive because they require sorting the data within each partition (departmentId in this case). Using MAX() with GROUP BY avoids this overhead by directly calculating the maximum salary for each department.

4. **row_num vs rank vs dense_rank?**

[See implementation in `rowNum-rank-denseRank.py`](../Spark/rowNum-rank-denseRank.py)

```
+---+-----+------+------------+-------+--------+--------------+
| id| name|salary|departmentId|row_num|rank_num|dense_rank_num|
+---+-----+------+------------+-------+--------+--------------+
|  4|  Max| 90000|           1|      1|       1|             1|
|  1|  Joe| 85000|           1|      2|       2|             2|
|  6|Randy| 85000|           1|      3|       2|             2|
|  7| Will| 70000|           1|      4|       4|             3|
|  5|Janet| 69000|           1|      5|       5|             4|
|  2|Henry| 80000|           2|      1|       1|             1|
|  3|  Sam| 60000|           2|      2|       2|             2|
+---+-----+------+------------+-------+--------+--------------+
```
Here, note that for Joe and Randy has different row_num but same rank and dense_rank. In the case of rank the counting skips to 4, while dense_rank it doesn't. 

### Python: 
1. What Python libraries have you used? 
- Pandas
- Boto3
- Pyspark
- BeautifulSoup
- Libraries to connect Databases from Python or Pull Data

2. Given an array of integers, move all the 0s to the back of the array while maintaining the relative order of the non-zero elements. 
Do this in-place using constant auxiliary space. 
Input: [1, 0, 2, 0, 0, 7] 
Output: [1, 2, 7, 0, 0, 0] 

Solution: 
Take four pointers, to store zero index, non zero index, 
last_zero_index and last_non_zero_index. Loop over array and whenever you find a non zero, put the value and last_non_zero_index and update values for last_zero_index and last_non_zero_index. 

[See implementation in `iqvia-move-zeroes.py`](../DSA/5-iqvia-move-zeroes.py)

3. **Reverse a string in python.**

```
for char in reversed(s):
    print(char, end="")
```
or simply do
```
s[::-1]
```
4. **Max number in python**
```
max(li)
```
5. **Slicer in python.**

```
sequence[start:stop:step]
```

6. **Memory Management in Python**

| Aspect | Description | Example/Tool |
| :-- | :-- | :-- |
| Memory Model | Python uses private heap space for all objects and data structures. | Managed by Python's memory manager. |
| Memory Allocation | Allocates memory for objects dynamically. | Integer preallocation (-5 to 256), string interning. |
| Reference Counting | Tracks the number of references to an object. | del x reduces reference count; memory is deallocated when count reaches 0. |
| Garbage Collection | Cleans up unused objects and handles circular references. | import gc; gc.collect() |
| Generational GC | Divides objects into generations for optimized garbage collection. | Younger objects are collected more frequently. |
| Stack Memory | Used for local variables and function calls. | Automatically allocated and deallocated. |
| Heap Memory | Used for objects and data structures. | Managed by Python's memory manager. |
| Object-Specific Allocators | Specialized allocators for integers, strings, etc. | Integer preallocation, string interning. |
| Use Generators | Produce items one at a time to save memory. | def my_generator(): yield i |
| Use __slots__ | Restrict attributes in classes to reduce memory usage. | class MyClass: __slots__ = ['name', 'age'] |
| Avoid Large Temporary Objects | Use in-place operations or reuse objects to save memory. | Avoid creating large intermediate lists. |
| Built-in Data Structures | Use optimized structures like list, dict, set. | Prefer built-in types over custom ones. |
| Profile Memory Usage | Monitor memory usage to identify bottlenecks. | memory_profiler, tracemalloc |
| gc Module | Provides access to garbage collection. | gc.enable(), gc.collect() |
| sys Module | Provides functions to check memory usage. | sys.getsizeof(x) |
| tracemalloc Module | Helps trace memory allocations. | tracemalloc.start(); tracemalloc.get_traced_memory() |
| Memory Leaks | Occur due to circular references or improper cleanup. | Use gc to detect and resolve leaks. |
| Excessive Memory Usage | Caused by large objects or inefficient data structures. | Optimize with generators, __slots__, and profiling tools. |
| Small File Problem | Too many small objects lead to fragmentation and overhead. | Use batching or combine small objects into larger ones. |

7. **static vs class functions**

| Aspect | Static Method | Class Method |
| :-- | :-- | :-- |
| Definition | Defined using @staticmethod decorator. | Defined using @classmethod decorator. |
| Binding | Bound to the class, but does not take any implicit arguments like self or cls. | Bound to the class and takes cls as the first argument. |
| Purpose | Used for utility functions that do not depend on the class or instance. | Used for methods that operate on the class itself, often to modify or access class-level data. |
| Access to Class/Instance | Cannot access class-level or instance-level attributes directly. | Can access and modify class-level attributes via cls. |
| Call | Called directly on the class or instance. | Called directly on the class or instance. |
| Use Case | General-purpose methods that are logically related to the class but do not require class or instance data. | Methods that need to work with class-level data or perform operations specific to the class. |

| Type | Decorator | First Argument | Access | Use Case |
| :-- | :-- | :-- | :-- | :-- |
| Instance Method | None | self | Can access instance and class data. | Operations specific to an instance. |
| Static Method | @staticmethod | None | Cannot access instance or class data. | Utility functions related to the class. |
| Class Method | @classmethod | cls | Can access and modify class data. | Operations related to the class itself. |

8. **Decorators in Python, any use outside Flask**

 Decorators can simplify code, enforce rules, optimize performance, and add functionality to functions and methods. By understanding their use cases, you can leverage decorators effectively in your Python projects!

| Use Case | Description | Example |
| :-- | :-- | :-- |
| Logging | Logs function calls, arguments, and return values. | Debugging or monitoring. |
| Access Control | Enforces user permissions or roles. | Admin-only functions. |
| Memoization | Caches results of expensive function calls. | Fibonacci or other recursive functions. |
| Input Validation | Validates inputs to a function. | Prevent negative values. |
| Timing | Measures execution time of functions. | Performance monitoring. |
| Retry Logic | Retries a function on failure. | Network or API calls. |
| Debugging | Prints function arguments and return values for debugging. | Debugging complex functions. |

9. **DataTypes in Python and when do we use Tuple over List**

| Category | Data Type | Example |
| :-- | :-- | :-- |
| Numeric Types | int, float, complex | 10, 3.14, 2 + 3j |
| Sequence Types | str, list, tuple, range | "Hello", [1, 2], (1, 2), range(5) |
| Mapping Type | dict | {"key": "value"} |
| Set Types | set, frozenset | {1, 2, 3}, frozenset([1, 2]) |
| Boolean Type | bool | True, False |
| Binary Types | bytes, bytearray, memoryview | b"data", bytearray(5) |
| None Type | NoneType | None |

| Use Case | Preferred Data Type |
| :-- | :-- |
| Data that should not change | Tuple |
| Data that needs to be modified | List |
| Fixed structure or record | Tuple |
| Dynamic size or frequently changing | List |
| Dictionary keys | Tuple |
| Better performance and memory usage | Tuple |
| Signaling intent for immutability | Tuple |

### Snowflake: 
1. **Transient Table in snowflake**

There are 4 types of tables in snowflake: 

| Feature | Permanent Table | Transient Table | Temporary Table | External Table |
| :-- | :-- | :-- | :-- | :-- |
| Persistence | Persistent across sessions. | Persistent across sessions. | Session-specific; dropped after session ends. | Data remains in external storage. |
| Time Travel | Supported (up to 90 days). | Supported (up to 90 days). | Not supported. | Not supported. |
| Fail-Safe | Supported (7 days). | Not supported. | Not supported. | Not applicable. |
| Storage Costs | Higher due to Fail-Safe. | Lower (no Fail-Safe). | Minimal (session-based). | No storage costs in Snowflake. |
| Use Case | Long-term data storage. | Short-term or intermediate data. | Temporary or session-specific data. | Querying external datasets. |

Temporary tables are session specific, transient tables are not. 
Permanent tables are fail safe, transient tables are not. 

2. **How will you run a logic when some rows are inserted in a table ?**

Way 1: 
> Using Streams and Tasks

-  Create a Stream to Track Changes: 
```
CREATE STREAM orders_stream ON TABLE orders;
```
- Create the Task: 
```
CREATE OR REPLACE TASK process_new_orders
WAREHOUSE = my_warehouse
SCHEDULE = '5 MINUTE'
AS
INSERT INTO processed_orders (order_id, customer_id, order_date)
SELECT order_id, customer_id, order_date
FROM orders_stream
WHERE METADATA$ACTION = 'INSERT';
```
- Start the Task:
```
ALTER TASK process_new_orders RESUME;
```
- Monitor the task

Way 2: Snowpipe :  But it works when files land in cloud e.g. S3
So, it can be combined with Tasks and Stored Procedures. 

Way 3: Stored Procedure

```
CREATE OR REPLACE PROCEDURE process_new_rows()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO processed_table (column1, column2)
    SELECT column1, column2
    FROM my_table
    WHERE processed = FALSE;

    RETURN 'Rows processed successfully';
END;
$$;
```


3. **Procedure and Triggers in snowflake**

Snowflake has Procedures but it doesn't has Trigger. Stored Procedures with Tasks are used t achieve this. 

4. **Materialized View vs Normal View**

| Feature | Materialized View | Normal View |
| :-- | :-- | :-- |
| Data Storage | Stores precomputed results physically. | Does not store data; only query definition. |
| Performance | Faster for repeated queries. | Slower for complex queries. |
| Data Freshness | May have a slight delay in reflecting changes. | Always up-to-date. |
| Storage Costs | Additional storage costs for precomputed results. | No additional storage costs. |
| Maintenance | Automatically maintained by Snowflake. | No maintenance required. |
| Use Case | Optimizing performance for repeated queries. | Simplifying query logic and abstraction. |
