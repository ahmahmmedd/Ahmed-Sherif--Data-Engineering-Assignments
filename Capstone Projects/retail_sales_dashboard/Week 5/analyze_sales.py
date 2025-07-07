import pandas as pd
import os
os.makedirs("output", exist_ok=True)
df = pd.read_csv('data/cleaned_sales_data.csv')
df['revenue'] = df['quantity'] * df['price']
df['profit'] = df['revenue'] - df['cost']
store_metrics = df.groupby('store_id').agg({
    'revenue': 'sum',
    'profit': 'sum',
    'quantity': 'sum'
}).reset_index()
store_metrics.rename(columns={
    'revenue': 'total_revenue',
    'profit': 'total_profit',
    'quantity': 'total_sales'
}, inplace=True)
store_metrics.to_csv('output/store_sales_metrics.csv', index=False)
bottom_5 = store_metrics.sort_values('total_revenue').head(5)
bottom_5.to_csv('output/bottom_5_stores.csv', index=False)
print("Sales analysis complete. Reports saved in")
