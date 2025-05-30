#7
class Employee:
    def __init__(self,name, department, salary):
        self.name = name
        self.department = department
        self.salary = salary

    def display(self):
        print(f"Name: {self.name}, Department: {self.department}, Salary: {self.salary}")

    def is_high_earner(self):
        return self.salary>60000

#8
class Project(Employee):
    def __init__(self,name, department, salary, project_name, hours_allocated):
        super().__init__(name, department, salary)
        self.project_name = project_name
        self.hours_allocated = hours_allocated

    def display(self):
        super().display()
        print(f"Project: {self.project_name}, Hours Allocated: {self.hours_allocated}")

#9
emp= Project("Ali", "HR", 50000, "AI Chatbot","120")
emp1= Project("Neha", "IT", 60000, "ERP System", 200)
emp2= Project("Sara", "IT", 70000, "Payroll", 150)

for i in [emp, emp1, emp2]:
    i.display()
    if i.is_high_earner():
        print(f"{i.name} is a high earner\n")
    else:
        print(f"{i.name} is a low earner\n")