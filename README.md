# Near Real-Time Data Warehouse Prototype for Electronics Business Chain Using Java & MySQL:

This repository comprises the design, implementation, and analysis of a near real-time data warehouse prototype for an electronics business chain, utilising a multi-threaded Extract, Transform, Load (ETL) pipeline leveraging the efficient HYBRIDJOIN algorithm implemented with Java and MySQL on customer sales data, as the final project for the Data Warehousing & Business Intelligence (DS3003) course.

### Dependencies:

* MySQL Community Server ([download](https://dev.mysql.com/downloads/mysql/))
* MySQL Workbench ([download](https://dev.mysql.com/downloads/workbench/))
* IntelliJ IDEA ([download](https://www.jetbrains.com/idea/download/))

## Introduction:

In this project, the objective is to design, implement, and analyse a data warehouse prototype for Electronica, one of the largest electronics business chains in Pakistan. The motivation behind this endeavour is to enable real-time analysis of customer shopping behaviour, facilitating informed business decisions and optimising selling techniques.

### Data Warehouse Structure:

Utilising a star schema methodology, the data modelling technique maps multidimensional decision support data into the relational database structure.

<img src="https://github.com/huzaifakhan04/near-real-time-data-warehouse-prototype-for-electronics-business-chain-using-java-and-mysql/blob/main/schema.png" alt="Star Schema" style="width: 100%; height: auto; display: block;">

### Data Sources:

Two comma-separated values (CSV) files, namely `transactions.csv` and `master_data.csv`, provide the transactional and master data, respectively.
* **Attributes `(transactions.csv)`:** *Order ID, Order Date, ProductID, CustomerID, CustomerName, Gender, Quantity Ordered*
* **Attributes `(master_date.csv)`:** *productID, productName, productPrice, supplierID, supplierName, storeID, storeName*

## Extract, Transform, Load (ETL) Process:

### Controller:

The **Controller** class plays a pivotal role in overseeing both the stream arrival rate and service rate within the system. This thread is tasked with dynamically regulating the operational pace of the **StreamGenerator** class, aligning it with the specified service rate. Its primary objective is to maintain an optimal workload for the HYBRIDJOIN algorithm, preventing underload scenarios and mitigating the risk of an unwarranted backlog in the stream buffer. Moreover, the **Controller** class employs a multi-threading approach to facilitate parallel execution of the **StreamGenerator** and **HybridJoin** classes, ensuring efficient and concurrent processing within the system.

### StreamGenerator:

The **StreamGenerator** class functions as the data generation module for the data warehouse, emulating a dynamic data-processing stream. Its primary responsibility lies in the extraction of data from comma-separated values (CSV) files and subsequent insertion into the pertinent dimension tables within the database. This class leverages an instantiated object of the **HybridJoin** class to orchestrate the join operation across dimension tables, employing the sophisticated HYBRIDJOIN algorithm. Through this collaborative process, the **StreamGenerator** class orchestrates the creation of the fact table within the data warehouse.

### HybridJoin:

The **HybridJoin** class assumes the critical role of executing the HYBRIDJOIN algorithm, aligning with the _**Hybrid join (METHOD=4)**_ methodology detailed in the [documentation](https://www.ibm.com/docs/en/db2-for-zos/11?topic=operations-hybrid-join-method4) for the Db2 11 for z/OS enterprise data server for IBM Z. This algorithm is tailored specifically for inner joins, necessitating the presence of an index on the join column of the inner table. A key prerequisite for its effective implementation involves acquiring Record Identifiers (RID) in the requisite order, optimising the utilisation of list prefetch mechanisms.

## HYBRIDJOIN Algorithm Methodology:

As elucidated in the Db2 11 for z/OS enterprise data server documentation by IBM Z, the _**Hybrid join (METHOD=4)**_ methodology meticulously executes the following procedural steps:

*	**Outer Table Scanning:** The algorithm initiates by scanning the outer table (OUTER), setting the foundation for subsequent join operations.
*	**Joining with Record Identifiers (RID) from Inner Table Index:** The outer table is systematically joined with Record Identifiers (RID) obtained from the index on the inner table. This culmination results in the creation of the Phase 1 intermediate table. Notably, the inner table index undergoes scanning for each row of the outer table.
*	**Sorting Outer Table Data and Record Identifiers (RID):** A pivotal sorting operation is performed on both the data in the outer table and the corresponding Record Identifiers (RID). This process yields a sorted Record Identifier (RID) list and the Phase 2 intermediate table. The necessity of this sort, indicated by a value of _**Y**_ in the column _**SORTN_JOIN**_ of the plan table, is circumvented if the inner table's index is well-clustered (_**SORTN_JOIN=N**_).
*	**Inner Table Data Retrieval Using List Prefetch:** Subsequently, data is retrieved from the inner table leveraging list prefetch techniques, contributing to the efficiency of the overall HYBRIDJOIN implementation.
*	**Concatenation of Inner Table Data and Phase 2 Intermediate Table:** The final step involves concatenating the data from the inner table with the Phase 2 intermediate table, culminating in the creation of the ultimate composite table.

<img src="https://www.ibm.com/docs/en/SSEPEK_11.0.0/perf/src/art/bkn9p034.gif" alt="Hybrid join (SORTN_JOIN='Y')" style="width: 100%; height: auto; display: block;">

This HYBRIDJOIN implementation excels in list prefetch utilisation, particularly outperforming nested loop joins when indexes on the join predicate exhibit low cluster ratios. Noteworthy efficiencies extend to the processing of duplicates, where the inner table is scanned only once for each set of duplicate values in the join column of the outer table. In instances where the inner table's index boasts high clustering, the sorting of the intermediate table becomes superfluous _**(SORTN_JOIN=N)**_, and the intermediate table resides in memory rather than a work file.

### Shortcomings:

While the HYBRIDJOIN algorithm offers advantages in certain scenarios, it also has limitations.

* **Dependency on Indexes:** HYBRIDJOIN requires an index on the join column of the inner table. If the necessary indexes are not present or are not well-maintained, the algorithm's performance may degrade significantly. The dependency on indexes can pose challenges in scenarios where maintaining indexes becomes resource-intensive or where the index is not effectively utilised due to its size or fragmentation.
* **Inner Table Clustering Requirement:** The algorithm's efficiency is influenced by the clustering of the index on the inner table. If the index is not well-clustered, it may necessitate additional sorting operations, impacting overall performance. While the algorithm can skip the sort step if the index is well-clustered, ensuring optimal clustering may not always be feasible, especially in dynamic or frequently updated datasets.

## Usage:

* `data` — Includes comma-separated values (CSV) files containing relevant customer sales data.
* `lib` — Contains all essential Java archive (.jar) files.
* `sql/createDW.sql` — Structured Query Language (SQL) code designed to construct the data warehouse.
* `sql/queriesDW.sql` — Structured Query Language (SQL) code for analysing the populated data warehouse through Online Analytical Processing (OLAP) queries.
* `src` — Contains Java class source code files necessary to run the Extract, Transform, Load (ETL) pipeline.

## Instructions (Execution):

* Run the `createDW.sql` file located in the `sql` folder using MySQL Workbench to create the **Electronica_DW** database.
* Launch IntelliJ IDEA, click on "Open," and open the project folder.
* In the toolbar, select "File" and then choose "Project Structure..."
* Navigate to the "Modules" tab, and click on the '+' sign to add a new module.
* Create a new module by selecting "Java" as the "Language", "IntelliJ" as the "Build system", and "1.8 AdoptOpenJDK (HotSpot) version 1.8.0_292" as the "JDK".
* After creating the module under your preferred name, click on it, go to the "Dependencies" tab, and click on the '+' sign.
* Choose the "1 JARs or Directories..." option and browse to the `lib` folder in the directory.
* Select all the Java archive (.jar) files and add them.
* Once added, click on "Apply" and then "OK".
* Open the `StreamGenerator.java` file from the `src` folder and navigate to Line #75 to adjust the MySQL server connection configurations.
* Open the `Controller.java` file from the `src` folder and run it.
*	After the execution is complete, open the `queriesDW.sql` file in the `sql` folder, and execute each analytical query individually in MySQL Workbench to obtain the corresponding results.

#### Note:

The project was developed exclusively on **macOS Venture Version 13.6**, and executing it on different operating systems may necessitate appropriate adjustments. Kindly refrain from altering the folder structure of the project folder, as the code files have been customised to align with the current project structure.

### References:

* Naeem, M. A., Dobbie, G., & Weber, G. (2011). HYBRIDJOIN for near-real-time data warehousing. _International Journal of Data Warehousing and Mining, 7_(4), 21+. https://link.gale.com/apps/doc/A279723178/AONE?u=anon~1ede8b55&sid=googleScholar&xid=04a0ecdb
* _Hybrid join (Method=4)_ (no date) Db2 11 - _Performance - Hybrid join (METHOD=4)_. Available at: https://www.ibm.com/docs/en/db2-for-zos/11?topic=operations-hybrid-join-method4 (Accessed: 19 November 2023).
