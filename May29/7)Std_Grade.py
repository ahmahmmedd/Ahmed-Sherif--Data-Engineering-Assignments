class Student:
    def __init__(self, name, marks):
        self.name = name
        self.marks = marks

    def average(self):
        return sum(self.marks) / len(self.marks)

    def grade(self):
        avg= self.average()
        if avg>= 90: return 'A'
        if avg>= 80: return 'B'
        if avg>= 70: return 'C'
        return 'F'

student = Student("Kristen", [99,84,72,89,66])
print(f"{student.name}'s average: {student.average()}")
print(f"{student.name}'s grade: {student.grade()}")