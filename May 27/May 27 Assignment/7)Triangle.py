a1 = int(input("Enter first side: "))
a2 = int(input("Enter second side: "))
a3 = int(input("Enter third side: "))

if a1 + a2 > a3 and a1 + a3 > a2 and a2 + a3 > a1:
    print("Valid triangle")
else:
    print("Not a valid triangle")