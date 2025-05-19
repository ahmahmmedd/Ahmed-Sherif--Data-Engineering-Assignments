use hexa;

create table book (
bookid int primary key,
title varchar(40) not null,
author varchar(50) NOT NULL,
genre varchar(30),
price int,
publishedyear int,
stock int);

insert into book values
(1, 'The Alchemist', 'Paulo Coelho', 'Fiction', 300, 1988, 50),
(2, 'Sapiens', 'Yuval Noah Harari', 'Non-Fiction', 500, 2011, 30),
(3, 'Atomic Habits', 'James Clear', 'Self-Help', 400, 2018, 25),
(4, 'Rich Dad Poor Dad', 'Robert Kiyosaki', 'Personal Finance', 350, 1997, 20),
(5, 'The Lean Startup', 'Eric Ries', 'Business', 450, 2011, 15);

/* Tasks:
1. CRUD Operations:
1) Add a new book:
Insert a book titled "Deep Work" by Cal Newport, Genre Self-Help, Price 420, Published Year 2016, Stock 35.*/

insert into book values
(6, 'Deep Work', 'Cal Newport', 'Self-Help', 420, 2016, 35);

/* Update book price:
2) Increase the price of all Self-Help books by 50.*/

update book
set price = price + 50
where genre = 'self-help';

/* Delete a book:
3) Remove the book with BookID = 4 (Rich Dad Poor Dad). */

delete from book where bookid = 4;

/* Read all books:
4) Display all books sorted by Title in ascending order. */

select * from book order by title asc;

/* 2. Sorting and Filtering:
Sort by price:
5) List books sorted by Price in descending order. */

select * from book order by price desc;

/* Filter by genre:
6) Display all books belonging to the Fiction genre. */

select * from book where genre = 'fiction';

/* Filter with AND condition:
7) List all Self-Help books priced above 400. */

select * from book 
where genre = 'self-help' and price > 400;

/*Filter with OR condition:
8) Retrieve all books that are either Fiction or published after 2000.*/

select * from book 
where genre = 'fiction' or publishedyear > 2000;

/* 3. Aggregation and Grouping:
Total stock value:
9) Calculate the total value of all books in stock (Price * Stock).*/ 

select sum(price * stock) as total_value from book;

/* Average price by genre:
10) Calculate the average price of books grouped by Genre. */

select genre, avg(price) as avg_price 
from book group by genre;

/* Total books by author:
11) Count the number of books written by Paulo Coelho. */

select count(*) as book_count 
from book where author = 'paulo coelho';

/* 4. Conditional and Pattern Matching:
Find books with a keyword in title:
12) List all books whose Title contains the word "The".*/

select * from book 
where title like '%The%';

/* Filter by multiple conditions:
13) Display all books by Yuval Noah Harari priced below 600. */ 

select * from book 
where author = 'yuval noah harari' and price < 600;

/* Find books within a price range:
14) List books priced between 300 and 500. */

select * from book 
where price between 300 and 500;

/* 5. Advanced Queries:
Top 3 most expensive books:
15) Display the top 3 books with the highest price. */

select * from book order by price desc limit 3;

/* Books published before a specific year:
16) Find all books published before the year 2000. */

select * from book where publishedyear < 2000;

/* Group by Genre:
17) Calculate the total number of books in each Genre. */ 

select genre, count(*) as book_count
from book group by genre;

/* Find duplicate titles:
18) Identify any books having the same title. */

select title, count(*) as count
from book group by title having count(*) > 1;

/* 6. Join and Subqueries (if related tables are present):
Author with the most books:
Find the author who has written the maximum number of books. */

select author, count(*) as book_count
from book group by author
order by book_count desc
limit 1;

/* Oldest book by genre:
Find the earliest published book in each genre. */

select genre, title, publishedyear
from book
where (genre, publishedyear) in (select genre, min(publishedyear)
from book
group by genre);