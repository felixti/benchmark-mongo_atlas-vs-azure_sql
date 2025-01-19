/*******************************************
Step 1: Create the Database and Use It
*******************************************/
IF DB_ID('BenchmarkDB') IS NOT NULL
BEGIN
    PRINT 'Dropping existing BenchmarkDB';
    ALTER DATABASE BenchmarkDB SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE BenchmarkDB;
END

CREATE DATABASE BenchmarkDB;
GO

USE BenchmarkDB;
GO

/*******************************************
Step 2: Create Tables with Constraints and Indexes
*******************************************/

/* Customers Table */
CREATE TABLE dbo.Customers (
    CustomerID INT IDENTITY(1,1) PRIMARY KEY,
    FirstName NVARCHAR(50) NOT NULL,
    LastName NVARCHAR(50) NOT NULL,
    Email NVARCHAR(100) NOT NULL UNIQUE,
    CreatedDate DATETIME NOT NULL DEFAULT GETDATE()
);
GO

/* Products Table */
CREATE TABLE dbo.Products (
    ProductID INT IDENTITY(1,1) PRIMARY KEY,
    ProductName NVARCHAR(100) NOT NULL,
    Price DECIMAL(10,2) NOT NULL CHECK (Price >= 0),
    CreatedDate DATETIME NOT NULL DEFAULT GETDATE()
);
GO

/* Orders Table */
CREATE TABLE dbo.Orders (
    OrderID INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID INT NOT NULL,
    OrderDate DATETIME NOT NULL DEFAULT GETDATE(),
    CONSTRAINT FK_Orders_Customers FOREIGN KEY (CustomerID)
        REFERENCES dbo.Customers(CustomerID)
);
GO

/* OrderDetails Table */
CREATE TABLE dbo.OrderDetails (
    OrderDetailID INT IDENTITY(1,1) PRIMARY KEY,
    OrderID INT NOT NULL,
    ProductID INT NOT NULL,
    Quantity INT NOT NULL CHECK (Quantity > 0),
    UnitPrice DECIMAL(10,2) NOT NULL CHECK (UnitPrice >= 0),
    CONSTRAINT FK_OrderDetails_Orders FOREIGN KEY (OrderID)
        REFERENCES dbo.Orders(OrderID),
    CONSTRAINT FK_OrderDetails_Products FOREIGN KEY (ProductID)
        REFERENCES dbo.Products(ProductID)
);
GO

/* Additional Indexes to Improve SELECT and JOIN Performance */
CREATE NONCLUSTERED INDEX IX_Customers_Email ON dbo.Customers(Email);
CREATE NONCLUSTERED INDEX IX_Orders_CustomerID_OrderDate ON dbo.Orders(CustomerID, OrderDate);
CREATE NONCLUSTERED INDEX IX_OrderDetails_OrderID_ProductID ON dbo.OrderDetails(OrderID, ProductID);
GO

/*******************************************
Step 3: Seed Data into Tables
*******************************************/

/*
We will seed:
 - 100,000 Customers
 - 1,000 Products
 - 500,000 Orders 
 - ~2,400,000 OrderDetails (~average 5 order details per order)

This totals roughly 3 million rows.
*/

SET NOCOUNT ON;

/* 3a. Insert Customers */
DECLARE @customerCount INT = 100000;
DECLARE @i INT = 1;

PRINT 'Seeding Customers...';
WHILE @i <= @customerCount
BEGIN
    INSERT INTO dbo.Customers (FirstName, LastName, Email, CreatedDate)
    VALUES (
        'FirstName' + CAST(@i AS NVARCHAR(10)),
        'LastName' + CAST(@i AS NVARCHAR(10)),
        'customer' + CAST(@i AS NVARCHAR(10)) + '@example.com',
        DATEADD(DAY, -ABS(CHECKSUM(NEWID())) % 365, GETDATE())
    );
    SET @i = @i + 1;
    -- Optionally print progress every 10000 rows
    IF @i % 10000 = 0
        PRINT CONCAT('Inserted ', @i, ' customers...');
END

/* 3b. Insert Products */
DECLARE @productCount INT = 1000;
DECLARE @j INT = 1;

PRINT 'Seeding Products...';
WHILE @j <= @productCount
BEGIN
    INSERT INTO dbo.Products (ProductName, Price, CreatedDate)
    VALUES (
        'Product' + CAST(@j AS NVARCHAR(10)),
        CAST((RAND(CHECKSUM(NEWID())) * 100 + 1) AS DECIMAL(10,2)),
        DATEADD(DAY, -ABS(CHECKSUM(NEWID())) % 365, GETDATE())
    );
    SET @j = @j + 1;
    IF @j % 200 = 0
        PRINT CONCAT('Inserted ', @j, ' products...');
END

/* 3c. Insert Orders */
DECLARE @orderCount INT = 500000;
DECLARE @orderCounter INT = 1;

PRINT 'Seeding Orders...';

-- We use a temporary table variable to store generated OrderIDs so that we can link OrderDetails.
-- However, for 500K rows, using a table variable might not be optimal.
-- Instead, we will generate OrderDetails on the fly in the next loop by retrieving the latest inserted OrderID.
-- **Note:** In a production seeding scenario, consider using SCOPE_IDENTITY() or OUTPUT clause in batches.

WHILE @orderCounter <= @orderCount
BEGIN
    DECLARE @randomCustomerID INT = (ABS(CHECKSUM(NEWID())) % @customerCount) + 1;
    INSERT INTO dbo.Orders (CustomerID, OrderDate)
    VALUES (
        @randomCustomerID,
        DATEADD(DAY, -ABS(CHECKSUM(NEWID())) % 365, GETDATE())
    );

    SET @orderCounter = @orderCounter + 1;

    IF @orderCounter % 50000 = 0
        PRINT CONCAT('Inserted ', @orderCounter, ' orders...');
END

/* 3d. Insert OrderDetails */
PRINT 'Seeding OrderDetails...';

-- For each order, insert between 1 and 5 order details.
DECLARE @currentOrder INT = 1;
DECLARE @maxOrderID INT;
SELECT @maxOrderID = MAX(OrderID) FROM dbo.Orders;

WHILE @currentOrder <= @maxOrderID
BEGIN
    DECLARE @detailCount INT = (ABS(CHECKSUM(NEWID())) % 5) + 1;  -- between 1 and 5 details per order
    DECLARE @d INT = 1;

    WHILE @d <= @detailCount
    BEGIN
        DECLARE @randomProductID INT = (ABS(CHECKSUM(NEWID())) % @productCount) + 1;
        DECLARE @unitPrice DECIMAL(10,2) = (SELECT Price FROM dbo.Products WHERE ProductID = @randomProductID);
        DECLARE @quantity INT = (ABS(CHECKSUM(NEWID())) % 10) + 1;  -- quantity from 1 to 10

        INSERT INTO dbo.OrderDetails (OrderID, ProductID, Quantity, UnitPrice)
        VALUES (
            @currentOrder,
            @randomProductID,
            @quantity,
            @unitPrice
        );

        SET @d = @d + 1;
    END

    SET @currentOrder = @currentOrder + 1;

    IF @currentOrder % 50000 = 0
        PRINT CONCAT('Inserted OrderDetails for ', @currentOrder, ' orders...');
END

PRINT 'Seeding Completed.';
GO

/*******************************************
Step 4: Sample Complex Query for Benchmark Testing
*******************************************/

/*
This sample query simulates a realistic workload where we:
 - Join the four tables
 - Use a filter on date and a search on customer email
 - Aggregate the order totals

You can use this query (and variations) for comparing performance.
*/

SELECT 
    c.CustomerID,
    c.FirstName,
    c.LastName,
    c.Email,
    COUNT(o.OrderID) AS OrdersCount,
    SUM(od.Quantity * od.UnitPrice) AS TotalSpent
FROM dbo.Customers AS c
INNER JOIN dbo.Orders AS o 
    ON c.CustomerID = o.CustomerID
INNER JOIN dbo.OrderDetails AS od 
    ON o.OrderID = od.OrderID
INNER JOIN dbo.Products AS p
    ON od.ProductID = p.ProductID
WHERE o.OrderDate >= DATEADD(DAY, -180, GETDATE())
    AND c.Email LIKE '%@example.com'
GROUP BY 
    c.CustomerID,
    c.FirstName,
    c.LastName,
    c.Email
ORDER BY TotalSpent DESC;
GO

