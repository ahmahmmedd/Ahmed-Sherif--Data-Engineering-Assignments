class Book:
    def __init__(self, title, author, price, in_stock):
        self.title = title
        self.author = author
        self.price = price
        self.in_stock = in_stock

    def sell(self, quantity):
        if quantity > self.in_stock:
            raise ValueError("Not enough stock")
        self.in_stock -= quantity
        return f"Sold {quantity} copies of '{self.title}'"

book=Book("Mongo DB", "Fathima", 50, 20)
try:
    print(book.sell(3))
    print(book.sell(23))
except ValueError as e:
    print("Error:", e)