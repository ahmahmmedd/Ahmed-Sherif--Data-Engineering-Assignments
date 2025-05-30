import pandas as pd
df = pd.read_csv('employees.csv')
employees = df[df['Department'] == 'IT']['Name']
with open('emp.txt', 'w') as f:
    for i in employees:
        f.write(i + '\n')