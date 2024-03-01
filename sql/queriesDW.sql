USE Electronica_DW;

/*  Q1: Present total sales of all products supplied by each supplier with
        respect to quarter and month using drill down concept.  */

SELECT s.supplierID,
    s.supplierName,
    t.Quarter,
    t.Month,
    SUM(t.`Quantity Ordered`*p.productPrice) AS Total_Sales
    FROM Supplier_Dimension s
    JOIN Product_Dimension p
    ON s.supplierID=p.supplierID
    JOIN Time_Dimension t
    ON p.productID=t.productID
    GROUP BY s.supplierID,
        s.supplierName,
        t.Quarter,
        t.Month,
        t.Year
    ORDER BY s.supplierID,
        t.Year,
        t.Quarter,
        t.Month;

/*  Q2: Find total sales of product with respect to month using feature of rollup on month and
        feature of dicing on supplier with name "DJI" and Year as "2019". You will use the
        grouping sets feature to achieve rollup. Your output should be sequentially ordered
        according to product and month. */

SELECT p.productID,
    p.productName,
    t.Month,
    s.supplierID,
    s.supplierName,
    SUM(t.`Quantity Ordered`*CAST(p.productPrice AS DECIMAL(10, 2))) AS Total_Sales
    FROM Product_Dimension p
    JOIN Supplier_Dimension s
    ON p.supplierID=s.supplierID
    JOIN Time_Dimension t
    ON p.productID=t.productID
    WHERE s.supplierName='DJI' AND t.Year=2019
    GROUP BY p.productID,
        p.productName,
        t.Month,
        s.supplierID,
        s.supplierName WITH ROLLUP
    ORDER BY p.productID,
        t.Month;

/*  Q3: Find the 5 most popular products sold over the weekends.    */

SELECT p.productID,
    p.productName,
    SUM(t.`Quantity Ordered`) AS Total_Quantity
    FROM Product_Dimension p
    JOIN Time_Dimension t
    ON p.productID=t.productID
    WHERE DAYOFWEEK(t.`Order Date`) IN (1, 7)
    GROUP BY p.productID,
        p.productName
    ORDER BY Total_Quantity DESC
    LIMIT 5;

/*  Q4: Present the quarterly sales of each product for 2019 along with its total yearly sales.
        Note: each quarter sale must be a column and yearly sale as well. Order result according
        to product  */

SELECT p.productID,
    p.productName,
    SUM(
        CASE WHEN t.Quarter=1
        THEN t.`Quantity Ordered`
        ELSE 0
        END
    ) AS Q1,
    SUM(
        CASE WHEN t.Quarter=2
        THEN t.`Quantity Ordered`
        ELSE 0
        END
    ) AS Q2,
    SUM(
        CASE WHEN t.Quarter=3
        THEN t.`Quantity Ordered`
        ELSE 0
        END
    ) AS Q3,
    SUM(
        CASE WHEN t.Quarter=4
        THEN t.`Quantity Ordered`
        ELSE 0
        END
    ) AS Q4,
    SUM(t.`Quantity Ordered`) AS Yearly_Total
    FROM Product_Dimension p
    JOIN Time_Dimension t
    ON p.productID=t.productID
    WHERE t.Year=2019
    GROUP BY p.productID,
        p.productName
    ORDER BY p.productID;

/*  Q5: Find an anomaly in the data warehouse dataset. write a query to show the anomaly and
        explain the anomaly in your project report. */

SELECT
    productID,
    productName,
    Total_Sales
    FROM (
        SELECT p.productID,
            p.productName,
            SUM(t.`Quantity Ordered`*CAST(p.productPrice AS DECIMAL(10, 2))) AS Total_Sales,
            AVG(SUM(t.`Quantity Ordered`*CAST(p.productPrice AS DECIMAL(10, 2))))
            OVER () AS Mean_Total_Sales,
            STDDEV_SAMP(SUM(t.`Quantity Ordered`*CAST(p.productPrice AS DECIMAL(10, 2))))
            OVER () AS Standard_Deviation_Total_Sales
            FROM Product_Dimension p
            JOIN Time_Dimension t
            ON p.productID=t.productID
            GROUP BY p.productID, p.productName
    ) AS Subquery
    WHERE ABS(Total_Sales-Mean_Total_Sales)>2*Standard_Deviation_Total_Sales;

/*  Q6: Create a materialised view with the name “STOREANALYSIS_MV” that presents the
        product-wise sales analysis for each store. */

--  Reference:  https://bobcares.com/blog/mysql-materialized-views/

DROP VIEW IF EXISTS StoreAnalysis_MV;

CREATE VIEW StoreAnalysis_MV AS
    SELECT s.storeID AS Store_ID,
        p.productID AS Product_ID,
        SUM(t.`Quantity Ordered`*CAST(p.productPrice AS DECIMAL(10, 2))) AS Store_Total
        FROM Store_Dimension s
        JOIN Product_Dimension p
        ON s.productID=p.productID
        JOIN Time_Dimension t
        ON p.productID=t.productID
        GROUP BY s.storeID,
            p.productID;

DROP EVENT IF EXISTS Refresh_Store_Analysis_MV;

CREATE EVENT IF NOT EXISTS Refresh_Store_Analysis_MV
    ON SCHEDULE EVERY 1 DAY
    DO
        BEGIN
            DROP VIEW IF EXISTS StoreAnalysis_MV;
            CREATE VIEW StoreAnalysis_MV AS
            SELECT s.storeID AS Store_ID,
                p.productID AS Product_ID,
                SUM(t.`Quantity Ordered`*CAST(p.productPrice AS DECIMAL(10, 2))) AS Store_Total
                FROM Store_Dimension s
                JOIN Product_Dimension p
                ON s.productID=p.productID
                JOIN Time_Dimension t
                ON p.productID=t.productID
                GROUP BY s.storeID,
                p.productID;
        END;
    ON COMPLETION NOT PRESERVE ENABLE;

SELECT * FROM StoreAnalysis_MV;

/*  Q7: Use the concept of Slicing calculate the total sales for the store “Tech Haven”and product
        combination over the months.    */

SELECT t.Month,
    p.productID,
    p.productName,
    SUM(t.`Quantity Ordered`*CAST(p.productPrice AS DECIMAL(10, 2))) AS Total_Sales
    FROM Store_Dimension s
    JOIN Product_Dimension p
    ON s.productID=p.productID
    JOIN Time_Dimension t
    ON p.productID=t.productID
    WHERE s.storeName='Tech Haven'
    GROUP BY t.Month,
        p.productID,
        p.productName;

/*  Q8: Create a materialized view named "SUPPLIER_PERFORMANCE_MV" that presents the
    monthly performance of each supplier.   */

DROP VIEW IF EXISTS Supplier_Performance_MV;

CREATE VIEW Supplier_Performance_MV AS
    SELECT s.supplierID,
        s.supplierName,
        t.Month,
        SUM(p.productPrice*t.`Quantity Ordered`) AS Monthly_Sales
        FROM Supplier_Dimension s
        JOIN Product_Dimension p
        ON s.supplierID=p.supplierID
        JOIN Time_Dimension t
        ON p.productID=t.productID
        GROUP BY s.supplierID,
            s.supplierName,
            t.Month;

DROP EVENT IF EXISTS Refresh_Supplier_Performance_MV;

CREATE EVENT IF NOT EXISTS Refresh_Supplier_Performance_MV
    ON SCHEDULE EVERY 1 DAY
    DO
        BEGIN
            DROP VIEW IF EXISTS Supplier_Performance_MV;
            CREATE VIEW Supplier_Performance_MV AS
            SELECT s.supplierID,
                s.supplierName,
                t.Month,
                SUM(p.productPrice*t.`Quantity Ordered`) AS Monthly_Sales
                FROM Supplier_Dimension s
                JOIN Product_Dimension p
                ON s.supplierID=p.supplierID
                JOIN Time_Dimension t
                ON p.productID=t.productID
                GROUP BY s.supplierID,
                    s.supplierName,
                    t.Month;
        END;
    ON COMPLETION NOT PRESERVE ENABLE;

SELECT * FROM Supplier_Performance_MV;

/*  Q9: Identify the top 5 customers with the highest total sales in 2019, considering the number
        of unique products they purchased.  */

SELECT c.CustomerID,
    c.CustomerName,
    COUNT(DISTINCT t.productID) AS Unique_Products,
    SUM(t.`Quantity Ordered`*CAST(p.productPrice AS DECIMAL(10, 2))) AS Total_Sales
    FROM Customer_Dimension c
    JOIN Time_Dimension t
    ON c.productID=t.productID
    JOIN Product_Dimension p
    ON t.productID=p.productID
    WHERE YEAR(t.`Order Date`)=2019
    GROUP BY c.CustomerID,
        c.CustomerName
    ORDER BY Total_Sales DESC
    LIMIT 5;

/*  Q10:    Create a materialized view named "CUSTOMER_STORE_SALES_MV" that presents the
            monthly sales analysis for each store and then customers wise.  */

DROP VIEW IF EXISTS Customer_Store_Sales_MV;

CREATE VIEW Customer_Store_Sales_MV AS
(
    SELECT s.storeID AS ID,
        'Store' AS Entity_Type,
        s.storeID AS Entity_ID,
        t.Month,
        SUM(t.`Quantity Ordered`*CAST(p.productPrice AS DECIMAL(10, 2))) AS Monthly_Sales
    FROM Store_Dimension s
    JOIN Product_Dimension p
    ON s.productID=p.productID
    JOIN Time_Dimension t
    ON p.productID=t.productID
    GROUP BY s.storeID,
        t.Month
    UNION
    SELECT c.CustomerID AS ID,
        'Customer' AS Entity_Type,
        c.CustomerID AS Entity_ID,
        t.Month,
        SUM(t.`Quantity Ordered`*CAST(p.productPrice AS DECIMAL(10, 2))) AS Monthly_Sales
    FROM Customer_Dimension c
    JOIN Time_Dimension t
    ON c.productID=t.productID
    JOIN Product_Dimension p
    ON t.productID=p.productID
    GROUP BY c.CustomerID,
        t.Month
);

DROP EVENT IF EXISTS Refresh_Customer_Store_Sales_MV;

CREATE EVENT IF NOT EXISTS Refresh_Customer_Store_Sales_MV
    ON SCHEDULE EVERY 1 DAY
    DO
        BEGIN
            DROP VIEW IF EXISTS Customer_Store_Sales_MV;
            CREATE VIEW Customer_Store_Sales_MV AS
            (
                SELECT s.storeID AS ID,
                    'Store' AS Entity_Type,
                    s.storeID AS Entity_ID,
                    t.Month,
                    SUM(t.`Quantity Ordered`*CAST(p.productPrice AS DECIMAL(10, 2))) AS Monthly_Sales
                FROM Store_Dimension s
                JOIN Product_Dimension p
                ON s.productID=p.productID
                JOIN Time_Dimension t
                ON p.productID=t.productID
                GROUP BY s.storeID,
                    t.Month
                UNION
                SELECT c.CustomerID AS ID,
                    'Customer' AS Entity_Type,
                    c.CustomerID AS Entity_ID,
                    t.Month,
                    SUM(t.`Quantity Ordered`*CAST(p.productPrice AS DECIMAL(10, 2))) AS Monthly_Sales
                FROM Customer_Dimension c
                JOIN Time_Dimension t
                ON c.productID=t.productID
                JOIN Product_Dimension p
                ON t.productID=p.productID
                GROUP BY c.CustomerID,
                    t.Month
            );
        END;
    ON COMPLETION NOT PRESERVE ENABLE;

SELECT * FROM Customer_Store_Sales_MV;