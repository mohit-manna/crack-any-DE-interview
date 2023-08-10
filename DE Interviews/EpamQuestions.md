

1. I have a huge list of integers in a file and that is put in a RDD- testRDD. And I wrote the following
code to compute average of the integers:
    ```
    def myAvg(x, y):
        return (x+y)/2.O;
    avg = testRDD.reduce(myAvg);
    ```
    A: The logic is incorrect

    B: Runtime error: ResourcePoolNotAvailable

    C: Average value is shown

    D: Weighted average is shown


2. As a DataEngineer you are asked to apply the schema for data while loading to Spark Dataframe.As per your observation, which of the following is not valid method

    A: StructField

    B: StructSchema ✔

    C: StructType

    D: StringType

3. The key inten for salting technique is:

    A: To increase the effective compute space by freeing up the executor memory

    B: To increase the efficiency of computing hash function by providing more input

    C: To balance key distribution

    D: Option a and b

    E: Option b and c ✔

    F: Option a and c

4. Which of the following are true of broadcast joins

    A: For a broadcast join to happen the broadcast keyword must be used on the broadcasting dataframe

    B: Broadcast join is enabled by default ✔

    C: Broadcast join can be enabled by `spark.sql.autoBroadcastJoinThreshold=-1`

    D: Broadcast join always sends the large dataframe to the small one if I put the broadcast keyword before the large dataframe.

5. Statement 1 : spark.sql("select * from table a join table b on a.id=b.id")

    Statement 2: df1.join(df2,df1.id == df2.id)

    Which of the following is true?

    A: Statement 1 is faster as its native SQL

    B: Statement 2 is faster as dataframe API is always spark intemal and evaluates quickly using catalyst optimizer

    C: Both are exact same in performance  ✔

    D: Both loads equal volume of data but dataframe API processes parallelly in a more efficient manner.

6. Spark off heap memory is used for

    A: Storing metadata cache
    -  Spark stores its metadata cache, such as table and partition metadata, in memory, but it is typically stored in the heap memory.

    B: Garbage collection
    - Garbage collection in Spark is handled by the Java Virtual Machine (JVM) and occurs in the heap memory, not off-heap.
    
    C: Intemal Java Purposes like String interning and JVM overheads
    - These operations also typically use the heap memory and are managed by the JVM.

    D: Heap spills when GIGC is used  ✔
    - When G1GC is used in Spark, it divides the heap into regions and performs garbage collection on smaller regions incrementally. In some scenarios, when there is not enough space left in the heap to process data, Spark may spill data from the heap to off-heap memory to avoid out-of-memory errors. This is known as heap spills, and it occurs in off-heap memory.


7. How many jobs will be created for a datasize of 1O GB while executing the following code:

    ```
    val df = spark.read.parquet(<path to file>)
    val dfl = df.map(function1).flatmap(<function2>).reduceByKey(<some key>).collect()
    ```

    A: 1

    B: 2

    C: 3

    D: 4

8. Which of the following are transformations?

    A: Take(n)

    B: Limit(n)

    C: countByValue()  ✔
    - This is a transformation. It returns a new RDD containing pairs of unique elements from the original RDD as keys and their respective counts as values.

    D: Top()

9. Which of the following is true for stateless transformation?

    A: Uses data or intermediate results from previous batches and computes the result of the current batch 
    - It is a stateful transformation, where data or intermediate results from previous batches are used to compute the result of the current batch.

    B: Windowed operations and updateStateByKey() are two type of Stateless transformation
    - It is stateful

    C: The processing of each batch has no dependency on the data of previous batches ✔

    D: None of the above

10. FlatMap transforms an RDD of length N into another RDD of length M. which of the following is true for N and M

    A: a and b ✔

    B: b and c

    C: c and a

    D: None

11. Which of the following is not true for Catalyst Optimizer?

    A: Catalyst optimizer makes use of pattern matching feature

    B: Catalyst contains the tree and the set of rules to manipulate the tree

    C: There are no specific libraries to process relational queries  ✔

    D: There are different rule sets which handle different phases of query

    -  Catalyst is a specific library within Apache Spark that is designed to process and optimize relational queries. It provides a framework for query optimization, including both logical and physical optimizations, and it contains various rule sets to manipulate the query plans effectively. Catalyst leverages pattern matching techniques and transformations to optimize the queries and generate efficient execution plans.

12. in spark sql optimization which of the following is not present in the logical plan?

    A. constant folding
    - Constant folding is an optimization technique where Spark SQL evaluates expressions with constant values during query optimization. It replaces expressions containing only constants with their computed results. This reduces unnecessary computations during query execution.

    B. abstract syntax tree ✔
    -  "abstract syntax tree" is used in parsing and analyzing the SQL statement, but it is not a part of the logical plan optimization process. It helps represent the structure of the SQL query, and once the parsing and analysis are complete, the logical plan is constructed to further optimize the query before execution

    C. projection pruning
    - Projection pruning is a technique where unnecessary columns (fields) are removed from the query's output. It is performed during query optimization to eliminate fields that are not used in subsequent operations, reducing data transfer and improving performance.

    D. predicate pushdow
    - Predicate pushdown is an optimization technique that pushes filtering conditions (predicates) as close to the data source as possible. It means filtering is performed early in the query execution process, reducing the amount of data that needs to be processed and improving performance.