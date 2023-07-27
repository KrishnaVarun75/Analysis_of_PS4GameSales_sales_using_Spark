# Analysis_of_PS4GameSales_using_Spark

In the repository we performed a total of 11 different analysis on the dataset (PS4 Games Sales) using Spark. 

Spark was used as it is developed to handle large-scale data processing tasks efficiently and is popular for several reasons:

Speed: Spark is designed for in-memory data processing, which significantly speeds up data processing compared to traditional 
disk-based processing systems like Hadoop MapReduce. It keeps intermediate data in memory, reducing the need for repeated 
read/write operations to disk, leading to faster data processing.

Ease of Use: Spark provides high-level APIs in Java, Scala, Python, and R, making it accessible to developers with different 
programming language preferences. It also offers an interactive shell for exploring data and running ad-hoc queries.

Versatility: Spark is a general-purpose data processing engine and supports various workloads like batch processing, interactive queries, 
real-time streaming, machine learning, and graph processing. This versatility allows organizations to consolidate multiple workloads into one platform.

Fault Tolerance: Spark provides fault tolerance through lineage information, allowing it to recompute lost data in case of node failures. 
This feature ensures data reliability and prevents data loss during processing.

Scalability: Spark scales horizontally, meaning it can distribute data processing across a cluster of computers. As the data and processing needs grow, Spark can efficiently handle increasing workloads by adding more machines to the cluster.

Unified Data Processing: With Spark, you can perform data processing across various data sources like Hadoop Distributed File System (HDFS), Apache Cassandra, Apache HBase, Amazon S3, and more. This unified data processing capability simplifies the data integration process.

Community and Ecosystem: Spark has a vibrant open-source community that contributes to its development and supports a wide range of libraries (e.g., Spark SQL, Spark Streaming, MLlib, GraphX) for different use cases, expanding its functionality and usability.

Cost-Effective: Spark can be run on commodity hardware, making it cost-effective for organizations to build large-scale data processing systems without investing in expensive infrastructure.
