class Rectangle:
    def __init__(self, length, width):
        self.length = length
        self.width = width

    def area(self):
        return self.length * self.width

    def perimeter(self):
        return 2 * (self.length + self.width)

    def is_square(self):
        return self.length == self.width


# Example usage:
rect = Rectangle(5, 5)
print("Area:", rect.area())
print("Perimeter:", rect.perimeter())
print("Is square?", rect.is_square())