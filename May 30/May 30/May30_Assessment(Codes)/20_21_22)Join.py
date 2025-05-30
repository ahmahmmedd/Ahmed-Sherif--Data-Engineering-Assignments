import pandas as pd

#20
emp = pd.read_csv('employees.csv')
pr = pd.read_csv('projects.csv')
merge = pd.merge(emp, pr, on='EmployeeID', how='inner')
print(merge)

#21
merged_all = pd.merge(emp, pr, on='EmployeeID', how='left')
no_project = merged_all[merged_all['ProjectName'].isnull()]
print(no_project)

#22
merged_all['TotalCost'] = merged_all['HoursAllocated'] * (merged_all['Salary'] / 160)
print(merged_all[['EmployeeID', 'Name', 'ProjectName', 'HoursAllocated', 'Salary', 'TotalCost']])

