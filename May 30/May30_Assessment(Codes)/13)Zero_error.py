def ZeroDivisionError (a, b):
    try:
        return a / b
    except ZeroDivisionError:
        return "Cannot divide with zero"

print(ZeroDivisionError (2, 0))