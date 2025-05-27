balance = 10000
while True:
    print("1. Deposit")
    print("2. Withdraw")
    print("3. Check Balance")
    print("4. Exit")
    choice = input("Enter your choice: ")
    if choice == '1':
        amount = float(input("Enter deposit amount: "))
        balance += amount
        print(f"New balance: {balance}")
    elif choice == '2':
        amount = float(input("Enter withdrawal amount: "))
        if amount > balance:
            print("Insufficient balance")
        else:
            balance -= amount
            print(f"Remaining balance: {balance}")
    elif choice == '3':
        print(f"Current balance: {balance}")
    elif choice == '4':
        break
    else:
        print("Invalid choice")