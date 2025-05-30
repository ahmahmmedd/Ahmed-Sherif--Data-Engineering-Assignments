try:
    num = float(input("Enter a number: "))
    print(f"Square: {num**2}")
except ValueError:
    print("Invalid input. Please enter a number.")