inp = input("Enter a string: ").lower()
if inp == inp[::-1]:
    print("It's a palindrome")
else:
    print("It's not a palindrome")