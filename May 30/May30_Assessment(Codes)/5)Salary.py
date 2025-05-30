salaries = [96000,24525,10000,100000,26000]
max_salary = max(salaries)
avg_salary = sum(salaries) / len(salaries)
above_avg = [s for s in salaries if s > avg_salary]
sorted_salaries = sorted(salaries, reverse=True)

print("Max salary:", max_salary)
print("Salaries above average:", above_avg)
print("Sorted salaries (descending):", sorted_salaries)