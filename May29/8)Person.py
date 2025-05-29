class Person:
    def __init__(self, name, age):
        self.name= name
        self.age= age

class Employee(Person):
    def __init__(self, name, age, emp_id, salary):
        super().__init__(name, age)
        self.emp_id= emp_id
        self.salary= salary

    def display_info(self):
        return (f"Name: {self.name}, Age: {self.age}, ID: {self.emp_id}, Salary: {self.salary}")

emp= Employee("Zahira",35,"101",500000)
print(emp.display_info())