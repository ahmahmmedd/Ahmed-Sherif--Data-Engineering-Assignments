num1 = float(input("Enter first number: "))
num2 = float(input("Enter second number: "))
a = input("Enter operator(+, -, *, /): ")

if a == '+':
    ans = num1 + num2
elif a == '-':
    ans = num1 - num2
elif a == '*':
    ans = num1 * num2
elif a == '/':
    ans = num1 / num2
print(f"ans: {ans}")