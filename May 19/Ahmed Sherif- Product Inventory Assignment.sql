use hexa;

create table productinventory 
(ProductID int primary key,
ProductName varchar(45) not null,
Category varchar(45),
Quantity int,
UnitPrice int,
Supplier varchar(50),
LastRestocked date);

insert into productinventory values
(1, 'Laptop', 'Electronics', 20, 70000, 'TechMart', '2025-04-20'),
(2, 'Office Chair', 'Furniture', 50, 5000, 'HomeComfort', '2025-04-18'),
(3, 'Smartwatch', 'Electronics', 30, 15000, 'GadgetHub', '2025-04-22'),
(4, 'Desk Lamp', 'Lighting', 80, 1200, 'BrightLife', '2025-04-25'),
(5, 'Wireless Mouse', 'Electronics', 100, 1500, 'GadgetHub', '2025-04-30');

/* Tasks:
1. CRUD Operations:
Add a new product:
1) Insert a product named "Gaming Keyboard", Category Electronics, Quantity 40, UnitPrice 3500, Supplier TechMart, LastRestocked 2025-05-01. */

insert into productinventory values
(6, 'Gaming Keyboard', 'Electronics', 40, 3500, 'TechMart', '2025-05-01');

/* Update stock quantity: 
2) Increase the Quantity of Desk Lamp by 20. */

update productinventory
set quantity=quantity + 20
where productname = 'Desk Lamp';

/* Delete a discontinued product:
3) Remove the product with ProductID = 2 (Office Chair). */

delete from productinventory where productid = 2;

/* Read all products:
4) Display all products sorted by ProductName in ascending order. */

select * from productinventory order by productname;

/* 2. Sorting and Filtering:
Sort by Quantity:
5) List products sorted by Quantity in descending order. */

select * from productinventory order by quantity desc;

/* Filter by Category:
6) Display all Electronics products. */

select * from productinventory where category = 'Electronics';

/* Filter with AND condition:
7) List all Electronics products with Quantity > 20. */

select * from productinventory 
where category = 'Electronics' and quantity > 20;

/* Filter with OR condition:
8) Retrieve all products that belong to Electronics or have a UnitPrice below 2000. */

select * from productinventory 
where category = 'Electronics' or unitprice < 2000;

/* 3. Aggregation and Grouping:
Total stock value calculation:
9) Calculate the total value of all products (Quantity * UnitPrice). */

select sum(quantity*unitprice) as total_value from productinventory;

/* Average price by category:
10) Find the average price of products grouped by Category. */

select category, avg(unitprice) as avg_price
from productinventory group by category;

/* Count products by supplier:
11) Display the number of products supplied by GadgetHub. */

select count(*) as product_count
from productinventory where supplier = 'GadgetHub';

/* 4. Conditional and Pattern Matching:
Find products by name prefix:
12) List all products whose ProductName starts with 'W'. */

select * from productinventory 
where productname like 'W%';

/* Filter by supplier and price:
13) Display all products supplied by GadgetHub with a UnitPrice above 10000. */

select * from productinventory 
where supplier='gadgethub' and unitprice>10000;

/* Filter using BETWEEN operator:
14) List all products with UnitPrice between 1000 and 20000. */

select * from productinventory 
where unitprice between 1000 and 20000;

/* 5. Advanced Queries:
Top 3 most expensive products:
15) Display the top 3 products with the highest UnitPrice. */

select * from productinventory 
order by unitprice desc limit 3;

/* Products restocked recently:
16) Find all products restocked in the last 10 days. */

select * from productinventory 
where lastrestocked >= current_date - interval 10 day;

/* Group by Supplier:
17) Calculate the total quantity of products from each Supplier. */

select supplier, sum(quantity) as total_quantity
from productinventory 
group by supplier;

/* Check for low stock:
18) List all products with Quantity less than 30. */

select * from productinventory 
where quantity < 30;

/* 6. Join and Subqueries (if related tables are present):
Supplier with most products:
19) Find the supplier who provides the maximum number of products. */

select supplier, count(*) as product_count
from productinventory
group by supplier
order by product_count desc
limit 1;

/* Product with highest stock value:
20) Find the product with the highest total stock value (Quantity * UnitPrice). */

select productname, (quantity * unitprice) as stock_value
from productinventory
order by stock_value desc
limit 1;



