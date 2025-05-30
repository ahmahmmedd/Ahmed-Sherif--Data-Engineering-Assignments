#17
import pandas as pd
df = pd.read_csv('employees.csv')
filtered_it = df[(df['Department'] == 'IT') & (df['Salary'] > 60000)]
print(filtered_it,"\n")

#18
grouped = df.groupby('Department').agg(
    Employeecount=('EmployeeID', 'count'),
    Totalsalary=('Salary', 'sum'),
    Averagesalary=('Salary', 'mean'))
print(grouped,"\n")

#19
sorted_df = df.sort_values(by='Salary', ascending=False)
print(sorted_df)
