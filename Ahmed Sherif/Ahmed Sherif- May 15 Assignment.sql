create database DE;
use DE;

create table Product (
ProductID int primary key,
ProductName varchar(35),
Category varchar(20),
Price int,
StockQuantity int,
Supplier varchar(40)
);

INSERT INTO Product VALUES
(1, 'Laptop', 'Electronics', 70000, 50, 'TechMart'),
(2, 'Office Chair', 'Furniture', 5000, 100, 'HomeComfort'),
(3, 'Smartwatch', 'Electronics', 15000, 200, 'GadgetHub'),
(4, 'Desk Lamp', 'Lighting', 1200, 300, 'BrightLife'),
(5, 'Wireless Mouse', 'Electronics', 1500, 250, 'GadgetHub');

/* Tasks:
1. CRUD Operations:
Add a new product:
1) Insert a product named "Gaming Keyboard" under the "Electronics" category, priced at 3500, with 150 units in stock, supplied by "TechMart".*/

insert into product values
(6, 'Gaming Keyboard', 'Electronics', 3500, 150, 'Techmart');

/* Update product price:
2) Increase the price of all Electronics products by 10%.*/

update product
set price = price * 1.10
where category = 'Electronics';

/*Delete a product:
3) Remove the product with the ProductID = 4 (Desk Lamp).*/

delete from product where productid = 4;

/*Read all products:
4) Display all products sorted by Price in descending order. */

select * from product order by price desc;

/* 2. Sorting and Filtering:
Sort products by stock quantity:
5) Display the list of products sorted by StockQuantity in ascending order.*/

select * from product order by stockquantity;

/* Filter products by category:
6) List all products belonging to the Electronics category. */

select * from product where category = 'Electronics';

/*Filter products with AND condition:
7) Retrieve all Electronics products priced above 5000.*/

select * from product 
where category ='Electronics' and price>5000;

/*Filter products with OR condition:
8) List all products that are either Electronics or priced below 2000.*/

select * from product 
where category ='Electronics' or price<2000;

/* 3. Aggregation and Grouping:
Calculate total stock value:
9) Find the total stock value (Price * StockQuantity) for all products.*/

select sum(price * stockquantity) as Total_Value from product;

/*Average price of each category:
10) Calculate the average price of products grouped by Category.*/

select category, avg(price) as avgprice 
from product group by category;

/*Total number of products by supplier:
11) Count the total number of products supplied by GadgetHub.*/

select count(*) as Product_Count 
from product where supplier = 'GadgetHub';

/* 4. Conditional and Pattern Matching:
Find products with a specific keyword:
12) Display all products whose ProductName contains the word "Wireless".*/

select * from product 
where productname like '%Wireless%';

/* Search for products from multiple suppliers:
13) Retrieve all products supplied by either TechMart or GadgetHub. */

select * from product 
where supplier in ('Techmart', 'GadgetHub');

/* Filter using BETWEEN operator:
14) List all products with a price between 1000 and 20000.*/

select * from product 
where price between 1000 and 20000;

/* 5. Advanced Queries:
Products with high stock:
15) Find products where StockQuantity is greater than the average stock quantity. */

select * from product 
where stockquantity >(select avg(stockquantity) from product);

/* Get top 3 expensive products:
16) Display the top 3 most expensive products in the table. */ 

select * from product 
order by price desc limit 3;

/* Find duplicate supplier names:
17) Identify any duplicate supplier names in the table. */

select supplier, count(*) as count 
from product group by supplier having count > 1;

/* Product summary:
18) Generate a summary that shows each Category with the number of products and the total stock value. */
select category, count(*) as Product_Count, sum(price * stockquantity) as Totalstockvalue
from product group by category;

/* 6. Join and Subqueries (if related tables are present):
Supplier with most products:
19) Find the supplier who provides the maximum number of products. */

select supplier, count(*) as ProductCount
from product
group by supplier
order by productcount desc
limit 1;

/*Most expensive product per category:
20) List the most expensive product in each category. */

select category, productname, price
from product
where (category, price) IN (
select category, MAX(price)
from product
group by category);
