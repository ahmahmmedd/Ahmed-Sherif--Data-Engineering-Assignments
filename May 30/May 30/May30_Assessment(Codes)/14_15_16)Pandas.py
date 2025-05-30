#14
import pandas as pd
from datetime import datetime
employees = pd.read_csv('employees.csv')
projects = pd.read_csv('projects.csv')

#15
print("First 2 rows:\n", employees.head(2))
print("\nUnique departments:", employees['Department'].unique())
print("\nAverage salary by department:\n", employees.groupby('Department')['Salary'].mean())

#16
current_year = datetime.now().year
employees['JoiningYear'] = pd.to_datetime(employees['JoiningDate']).dt.year
employees['TenureInYears'] = current_year - employees['JoiningYear']