import re
password = input("Enter a password: ")
number = any(char.isdigit() for char in password)
upper = any(char.isupper() for char in password)
symbol = bool(re.search(r'[!@#$%^&*(),.?":{}|<>]', password))

if len(password) >= 8 and number and upper and symbol:
    print("Password is strong")
else:
    print("Password is weak. It should:")
    if len(password) < 8:
        print("- Be at least 8 characters long")
    if not number:
        print("- Contain at least one number")
    if not upper:
        print("- Contain at least one uppercase letter")
    if not symbol:
        print("- Contain at least one special character")