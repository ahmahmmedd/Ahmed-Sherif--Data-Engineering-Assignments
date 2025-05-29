class BankAccount:
    def __init__(self, name, balance=0):
        self.name = name
        self.balance = balance

    def deposit(self, amount):
        self.balance += amount
        return f"Deposited {amount}. New balance: {self.balance}"

    def withdraw(self, amount):
        if amount > self.balance:
            return "Insufficient funds"
        self.balance -= amount
        return f"Withdrew {amount}. Remaining balance: {self.balance}"

    def get_balance(self):
        return f"Current balance: {self.balance}"


account = BankAccount("Anderson Silva", 3500)
print(account.deposit(500))
print(account.withdraw(200))
print(account.withdraw(5000))
print(account.get_balance())
