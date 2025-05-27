total = float(input("Enter total bill amount: "))
peep = int(input("Enter number of people: "))
tip = float(input("Enter tip percentage (0-100): "))

res = total * (1 + tip/100)
per_person = res / peep
print(f"Each person should pay: {per_person:.2f}")