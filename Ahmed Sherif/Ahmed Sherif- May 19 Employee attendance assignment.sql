use Employee_Attendance_Table;

create table employee_attendance (
    AttendanceID int primary key,
    EmployeeName varchar(35) not null,
    Department varchar(25),
    Date date,
    Status varchar(10),
    HoursWorked int
);

insert into employee_attendance values
(1, 'John Doe', 'IT', '2025-05-01', 'Present', 8),
(2, 'Priya Singh', 'HR', '2025-05-01', 'Absent', 0),
(3, 'Ali Khan', 'IT', '2025-05-01', 'Present', 7),
(4, 'Riya Patel', 'Sales', '2025-05-01', 'Late', 6),
(5, 'David Brown', 'Marketing', '2025-05-01', 'Present', 8);

/* Tasks:
1. CRUD Operations:
1) Add a new attendance record:
Insert a record for Neha Sharma, from Finance, on 2025-05-01, marked as Present, with 8 hours worked.*/ 

insert into employee_attendance values
(6, 'Neha Sharma', 'Finance', '2025-05-01', 'Present', 8);

/* Update attendance status:
2) Change Riya Patel's status from Late to Present. */

update employee_attendance
set status = 'Present'
where employeename = 'riya patel';

/* Delete a record:
3) Remove the attendance entry for Priya Singh on 2025-05-01. */

delete from employee_attendance 
where employeename = 'priya singh' and date = '2025-05-01';

/* Read all records:
4) Display all attendance records sorted by EmployeeName in ascending order. */

select * from employee_attendance order by employeename;

/* 2. Sorting and Filtering:
Sort by Hours Worked:
5) List employees sorted by HoursWorked in descending order. */

select * from employee_attendance 
order by hoursworked desc;

/* Filter by Department:
6) Display all attendance records for the IT department. */

select * from employee_attendance where department = 'IT';

/* Filter with AND condition:
7) List all Present employees from the IT department. */

select * from employee_attendance 
where status = 'Present' and department = 'IT';

/* Filter with OR condition:
8) Retrieve all employees who are either Absent or Late. */

select * from employee_attendance 
where Status = 'Absent' or Status = 'Late';

/* Aggregation and Grouping:
Total Hours Worked by Department:
9) Calculate the total hours worked grouped by Department. */

select department, sum(HoursWorked) as Total_Hours
from employee_attendance group by department;

/* Average Hours Worked:
10) Find the average hours worked per day across all departments. */

select avg(hoursworked) as avg_hours from employee_attendance;

/* Attendance Count by Status:
11) Count how many employees were Present, Absent, or Late. */

select status, count(*) as employee_count
from employee_attendance group by status;

/* 4. Conditional and Pattern Matching:
Find employees by name prefix:
12) List all employees whose EmployeeName starts with 'R'. */

select * from employee_attendance 
where employeename like 'R%';

/* Filter by multiple conditions:
13) Display employees who worked more than 6 hours and are marked Present. */

select * from employee_attendance 
where hoursworked > 6 and status = 'Present';

/* Filter using BETWEEN operator:
14) List employees who worked between 6 and 8 hours. */

select * from employee_attendance 
where hoursworked between 6 and 8;

/* 5. Advanced Queries:
Top 2 employees with the most hours:
15) Display the top 2 employees with the highest number of hours worked. */

select * from employee_attendance 
order by hoursworked desc limit 2;

/* Employees who worked less than the average hours:
16) List all employees whose HoursWorked are below the average. */

select * from employee_attendance 
where hoursworked < (select avg(hoursworked) from employee_attendance);

/* Group by Status:
17) Calculate the average hours worked for each attendance status (Present, Absent, Late). */

select status, avg(hoursworked) as avg_hours
from employee_attendance group by status;

/* Find duplicate entries:
18) Identify any employees who have multiple attendance records on the same date. */

select employeename, date, count(*) as count
from employee_attendance
group by employeename, date
having count(*) > 1;

/* 6. Join and Subqueries (if related tables are present):
Department with most Present employees:
19) Find the department with the highest number of Present employees. */

select department, count(*) as present_count
from employee_attendance
where status = 'Present'
group by department
order by present_count desc
limit 1;

/* Employee with maximum hours per department:
20) Find the employee with the most hours worked in each department. */

select department, employeename, hoursworked
from employee_attendance
where (department, hoursworked) in 
(select department, max(hoursworked)
from employee_attendance
group by department);