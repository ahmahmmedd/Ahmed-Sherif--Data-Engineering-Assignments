import pandas as pd
import numpy as np

# 1. Load CSV data
sales_df = pd.read_csv('sales.csv')
products_df = pd.read_csv('products.csv')

# 2. Clean data
# Drop rows with missing key values
sales_df.dropna(subset=['store_id', 'product_id', 'quantity', 'price', 'cost'], inplace=True)

sales_df['store_id'] = sales_df['store_id'].astype(int)
sales_df['product_id'] = sales_df['product_id'].astype(int)
sales_df['quantity'] = sales_df['quantity'].astype(int)
sales_df['price'] = sales_df['price'].astype(float)
sales_df['cost'] = sales_df['cost'].astype(float)
sales_df['discount'] = sales_df['discount'].fillna(0).astype(float)  # Fill missing discounts with 0

# 3. Calculate revenue, discount %, profit margin
sales_df['revenue'] = sales_df['quantity'] * (sales_df['price'] - sales_df['discount'])
sales_df['total_cost'] = sales_df['quantity'] * sales_df['cost']
sales_df['profit'] = sales_df['revenue'] - sales_df['total_cost']

def calculate_profit_margin(row):
    if row['revenue'] > 0:
        return (row['profit'] / row['revenue']) * 100
    else:
        return 0

sales_df['profit_margin_pct'] = sales_df.apply(calculate_profit_margin, axis=1)


# 4. Merge with product names
merged_df = pd.merge(sales_df, products_df, on='product_id', how='left')

# 5. Summary by product and store
summary = merged_df.groupby(['store_id', 'name'])[['revenue', 'profit']].sum().reset_index()

# Display result
print("Revenue and Profit Summary by Product and Store ")
print(summary)

sales_df.to_csv('cleaned_sales_data.csv', index=False)
summary.to_csv('summary_report.csv', index=False)
