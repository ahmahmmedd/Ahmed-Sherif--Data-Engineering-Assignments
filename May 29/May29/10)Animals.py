class Animal:
    def speak(self):
        return "An animal sound"

class Dog(Animal):
    def speak(self):
        return "Woof"

class Cat(Animal):
    def speak(self):
        return "Meow"

class Cow(Animal):
    def speak(self):
        return "Mooooooo"

animals = [Dog(), Cat(), Cow()]
for animal in animals:
    print(animal.speak())