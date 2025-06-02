create database RetailSalesPerformance;

use RetailSalesPerformance;

--- Create MySQL tables for products, stores, sales, and employees

create table Products(
ProductID int primary key,
name varchar(120),
category varchar(50),
sellingprice decimal(10,2),
costprice decimal(10,2)
);

create table Stores(
StoreID int primary key,
name varchar(130),
region varchar(40)
);

create table Employees(
EmployeeID int primary key,
name varchar(100),
role varchar(50),
StoreID int,
foreign key (StoreID) references Stores(StoreID)
);

create table Sales(
SaleID int primary key identity(1,1),
ProductID int,
StoreID int,
EmployeeID int,
quantity int,
saleDate DATE,
foreign key (ProductID) references Products(ProductID),
foreign key (StoreID) references Stores(StoreID),
foreign key (EmployeeID) references Employees(EmployeeID)
);

-- Insert sales data with CRUD operations

insert into Products (ProductID, name, category, sellingprice, costprice) VALUES
(101, 'Plate', 'Kitchen', 300, 240),
(102, 'Soap', 'Toiletries', 50, 20),
(103, 'Laptop', 'electronics', 50000, 40000),
(104, 'Pen', 'Stationery', 10, 4),
(105, 'T-Shirt', 'Apparel', 400, 300);

insert into Stores (StoreID, name, region) VALUES
(1, 'Reliance', 'East'),
(2, 'Bigbazar', 'West'),
(3, 'Dmart', 'North');


insert into Employees (EmployeeID, name, role, StoreID) VALUES
(1, 'Aditya', 'Cashier', 1),
(2, 'Bharath', 'Sales Associate', 1),
(3, 'Christo', 'Manager', 2),
(4, 'David', 'Cashier', 2),
(5, 'Ethan', 'Sales Associate', 3);


insert into Sales (ProductID, StoreID, EmployeeID, quantity, saleDate) VALUES
(101, 1, 1, 10, '2025-06-01'),
(102, 1, 2, 5, '2025-06-01'),
(103, 2, 3, 8, '2025-06-02'),
(101, 2, 4, 6, '2025-06-02'),
(104, 3, 5, 7, '2025-06-03'),
(102, 1, 2, 3, '2025-06-03'),
(105, 3, 5, 9, '2025-06-04');



-- Add indexes to search by product and region
create nonclustered index idx_product_name on Products(name)
create nonclustered index idx_region on Stores(region);



-- Write a stored procedure to calculate daily sales for a store

if OBJECT_ID('GetDailySalesByStore', 'P') IS NOT NULL
    drop procedure GetDailySalesByStore;
go


create procedure GetDailySalesByStore
    @StoreID int,
    @SaleDate DATE
as
begin
    select 
        p.name as ProductName,
        s.StoreID,
        sum(sa.quantity) as TotalQuantitySold,
        sum(sa.quantity * p.sellingprice) as TotalRevenue,
        sum((p.sellingprice - p.costprice) * sa.quantity) as TotalProfit
    from Sales sa
    join Products p on sa.ProductID = p.ProductID
    join Stores s on sa.StoreID = s.StoreID
    where sa.StoreID = @StoreID and sa.saleDate = @SaleDate
    group by p.name, s.StoreID;
end;
go


exec GetDailySalesByStore @StoreID = 3, @SaleDate = '2025-06-03';
