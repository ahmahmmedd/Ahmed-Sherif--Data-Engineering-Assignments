import pandas as pd
import numpy as np

attendance_df = pd.read_csv("attendance.csv")
tasks_df = pd.read_csv("tasks.csv")

attendance_df['Date'] = pd.to_datetime(attendance_df['Date'], dayfirst=True)
attendance_df['Clock_in'] = pd.to_datetime(attendance_df['Clock_in'], dayfirst=True)
attendance_df['Clock_out'] = pd.to_datetime(attendance_df['Clock_out'], dayfirst=True, errors='coerce')

attendance_df = attendance_df.assign(Clock_out=attendance_df['Clock_out'].fillna(pd.NaT))

attendance_df['work_hours'] = np.where(
    attendance_df['Clock_out'].notna(),
    (attendance_df['Clock_out'] - attendance_df['Clock_in']).dt.total_seconds() / 3600,
    np.nan
)

tasks_df['Assigned_Date'] = pd.to_datetime(tasks_df['Assigned_Date'], dayfirst=True)
tasks_df['Completed_Date'] = pd.to_datetime(tasks_df['Completed_Date'], dayfirst=True, errors='coerce')

merged_df = pd.merge(
    attendance_df,
    tasks_df.groupby(['Employee_ID', 'Assigned_Date'])['Task_ID'].count().rename('tasks_completed'),
    left_on=['Employee_ID', 'Date'],
    right_on=['Employee_ID', 'Assigned_Date'],
    how='left'
)

merged_df['productivity_score'] = merged_df['tasks_completed'] / merged_df['work_hours']
merged_df['productivity_score'] = merged_df['productivity_score'].replace([np.inf, -np.inf], np.nan)

summary = merged_df.groupby('Employee_ID').agg({
    'work_hours': 'mean',
    'productivity_score': 'mean',
    'Date': 'count'
}).rename(columns={'Date': 'days_worked'})

top_performers = summary.nlargest(5, 'productivity_score')
frequent_absentees = summary[summary['days_worked'] < summary['days_worked'].max()].nsmallest(5, 'days_worked')

report = f"""
Employee Performance Report
---------------------------

Top Performers (by productivity score):
{top_performers[['work_hours', 'productivity_score']]}

Frequent Absentees:
{frequent_absentees[['days_worked', 'work_hours']]}

Overall Statistics:
- Average work hours per day: {summary['work_hours'].mean():.2f}
- Average productivity score: {summary['productivity_score'].mean():.2f}
"""

print(report)

merged_df.to_csv('cleaned_attendance_tasks.csv', index=False)
with open('performance_report.txt', 'w') as f:
    f.write(report)