create database employee_tracker;
use employee_tracker;

create table Employees (
Employee_ID int primary key auto_increment,
Name varchar(50) not null,
Department varchar(50) not null,
Hire_Date date not null);

create table attendance (
Attendance_ID int primary key auto_increment,
Employee_ID int not null,
Date date not null,
Clock_in datetime not null,
Clock_out datetime,
foreign key (employee_id) references employees(employee_id));

create table tasks (
Task_ID int primary key auto_increment,
Employee_ID int not null,
Task_Name varchar(100) not null,
Assigned_Date date not null,
Completed_Date date,
Status enum('pending', 'in progress', 'completed') default 'pending',
foreign key (employee_id) references employees(employee_id)
);

/*CRUD Operations*/
/* sample data */
insert into employees (name, department, hire_date) values ('Fathima Zahira', 'HR', '2025-02-01');
insert into employees (name, department, hire_date) values ('Fayaz Ahmed', 'Tester', '2024-04-01');
insert into employees (name, department, hire_date) values ('Raiz Nurmagomedov', 'Tech', '2024-05-01');

insert into attendance (employee_id, date, clock_in) values (1, curdate(), now());
insert into attendance (employee_id, date, clock_in) values (5, curdate(), now());
insert into attendance (employee_id, date, clock_in) values (6, curdate(), now());

insert into tasks (employee_id, task_name, assigned_date, status) values (1, 'Entire Monthly Report', curdate(), 'pending');
insert into tasks (employee_id, task_name, assigned_date, status) values (5, 'Test the demo and report', curdate(), 'pending');
insert into tasks (employee_id, task_name, assigned_date, status) values (6, 'Create a secure Database', curdate(), 'pending');

/*read operations */

-- Today's attendance of employees
select e.employee_id, e.name, a.clock_in, a.clock_out
from employees e left join attendance a 
on e.employee_id = a.employee_id and a.date = curdate();

-- Checking Employees clocked in but not clocked out yet
select * from attendance 
where date = curdate() and clock_out is null;

-- Employees with pending task
select employees.employee_id, name, task_name, assigned_date 
from tasks join employees on employees.employee_id = tasks.employee_id
where status ='pending';

/*update operations */

-- update clock out of employee
update attendance 
set clock_out = now() 
where employee_id = 1 and date = curdate();

-- update task status to 'completed'
update tasks 
set status = 'completed', completed_date = curdate() 
where task_id = 1;

-- correct a mistaken clock-in time
update attendance 
set clock_in = '2025-06-25 08:58:00' 
where attendance_id = 1;

/* delete operations */

-- delete an attendance record 
delete from attendance where attendance_id = 5;

-- remove a canceled task
delete from tasks where task_id = 6;

-- delete all attendance records for a former employee
delete from attendance where employee_id = 5;

-- stored procedure to calculate working hours
delimiter //
create procedure calculateworkinghours(in emp_id int, in start_date date, in end_date date)
begin
select 
	e.employee_id,
	e.name,
	sum(timestampdiff(hour, a.clock_in, a.clock_out)) as total_hours
from attendance a
join employees e on a.employee_id = e.employee_id
where 
	a.employee_id = emp_id 
	and a.date between start_date and end_date
	and a.clock_out is not null
group by e.employee_id, e.name;
end //
delimiter ;

-- eg operation on procedure
call calculateworkinghours(1, '2025-01-01', '2025-12-31');