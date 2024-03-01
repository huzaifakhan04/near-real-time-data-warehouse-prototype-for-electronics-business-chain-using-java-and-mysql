--  Creating the `Electronica_DW` database for the data warehouse.

DROP DATABASE IF EXISTS Electronica_DW;

CREATE DATABASE Electronica_DW;

USE Electronica_DW;

--  Creating the `Supplier_Dimension` table to store the supplier information.

DROP TABLE IF EXISTS Supplier_Dimension;

CREATE TABLE Supplier_Dimension (
    supplierID INT PRIMARY KEY,
    supplierName VARCHAR(255)
);

--  Creating the `Product_Dimension` table to store the product information.

DROP TABLE IF EXISTS Product_Dimension;

CREATE TABLE Product_Dimension (
    productID INT PRIMARY KEY,
    productName VARCHAR(255),
    supplierID INT,
    productPrice VARCHAR(255),
    FOREIGN KEY (supplierID) REFERENCES Supplier_Dimension(supplierID)
);

/*  Creating a trigger to convert the product price from a string to a decimal
    value on insertion. */

DROP TRIGGER IF EXISTS Transform_Product_Dimension;

DELIMITER //
CREATE TRIGGER Transform_Product_Dimension
    BEFORE INSERT ON Product_Dimension
    FOR EACH ROW
    BEGIN
        SET NEW.productPrice=REPLACE(NEW.productPrice, '$', '');    --  Removing the Dollar sign.
        SET NEW.productPrice=CAST(NEW.productPrice AS DECIMAL(10, 2));  --  Converting the string to a decimal value.
    END;
//
DELIMITER ;

--  Creating the `Customer_Dimension` table to store the customer information.

DROP TABLE IF EXISTS Customer_Dimension;

CREATE TABLE Customer_Dimension (
    CustomerID INT PRIMARY KEY,
    CustomerName VARCHAR(255),
    Gender VARCHAR(10),
    productID INT
);

--  Creating the `Time_Dimension` table to store the order information.

DROP TABLE IF EXISTS Time_Dimension;

CREATE TABLE Time_Dimension (
    Time_ID INT PRIMARY KEY AUTO_INCREMENT,
    `Order ID` INT,
    `Order Date` VARCHAR(255),
    Hour INT,
    Minute INT,
    Day INT,
    Year INT,
    Quarter INT,
    Month INT,
    `Quantity Ordered` INT,
    productID INT
);

/*  Creating a trigger to convert the order date from a string to a date
    value on insertion. The trigger also extracts the hour, minute, day, year,
    quarter, and month from the date value. */

DROP TRIGGER IF EXISTS Transform_Time_Dimension;

DELIMITER //
CREATE TRIGGER Transform_Time_Dimension
    BEFORE INSERT ON Time_Dimension
    FOR EACH ROW
    BEGIN
        SET NEW.`Order Date`=STR_TO_DATE(NEW.`Order Date`, '%m/%d/%y %H:%i');   --  Converting the string to a date value.
        
        /*  Extracting the hour, minute, day, year, quarter, and month from the date value,
            and storing them in their respective columns.   */
        
        SET NEW.Hour=HOUR(NEW.`Order Date`);
        SET NEW.Minute=MINUTE(NEW.`Order Date`);
        SET NEW.Day=DAY(NEW.`Order Date`);
        SET NEW.Month=MONTH(NEW.`Order Date`);
        SET NEW.Year=YEAR(NEW.`Order Date`);
        SET NEW.Quarter=QUARTER(NEW.`Order Date`);
    END;
//
DELIMITER ;

--  Creating the `Store_Dimension` table to store the store information.

DROP TABLE IF EXISTS Store_Dimension;

CREATE TABLE Store_Dimension (
    storeID INT PRIMARY KEY,
    storeName VARCHAR(255),
    productID INT
);

--  Creating the `Sales_Fact` table to store the aggregated sales information.

DROP TABLE IF EXISTS Sales_Fact;

CREATE TABLE Sales_Fact (
    productID INT DEFAULT 0,
    CustomerID INT DEFAULT 0,
    Time_ID INT DEFAULT 0,
    storeID INT DEFAULT 0,
    Total_Sale DECIMAL(10, 2) DEFAULT 0.0,
    PRIMARY KEY (productID, CustomerID, Time_ID, storeID),
    FOREIGN KEY (productID) REFERENCES Product_Dimension(productID),
    FOREIGN KEY (CustomerID) REFERENCES Customer_Dimension(CustomerID),
    FOREIGN KEY (Time_ID) REFERENCES Time_Dimension(Time_ID)
);

--  Creating indexes on the `Sales_Fact` table to improve performance.

CREATE INDEX Sale_Time_Index ON Sales_Fact(Time_ID);
CREATE INDEX Sale_Product_Index ON Sales_Fact(productID);
CREATE INDEX Sale_Customer_Index ON Sales_Fact(CustomerID);
CREATE INDEX Sale_Store_Index ON Sales_Fact(storeID);
CREATE INDEX Sale_Total_Index ON Sales_Fact(Total_Sale);

/*  Creating a trigger to calculate the total sale of a product on insertion
    using the quantity ordered from the `Time_Dimension` table and the product
    price from the `Product_Dimension` table.   */

DROP TRIGGER IF EXISTS Calculate_Total_Sale;

DELIMITER //
CREATE TRIGGER Calculate_Total_Sale
    BEFORE INSERT ON Sales_Fact
    FOR EACH ROW
    BEGIN
        DECLARE Sale_Price DECIMAL(10, 2);
        DECLARE Quantity_Ordered INT;
        SELECT `Quantity Ordered` INTO Quantity_Ordered
        FROM Time_Dimension
        WHERE Time_ID=NEW.Time_ID;
        SELECT p.productPrice INTO Sale_Price
        FROM Product_Dimension p
        WHERE p.productID=NEW.productID;
        SET NEW.Total_Sale=Quantity_Ordered*Sale_Price;
    END;
//
DELIMITER ;