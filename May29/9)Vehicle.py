class Vehicle:
    def __init__(self, name, wheels):
        self.name = name
        self.wheels = wheels

    def description(self):
        return f"{self.name} with {self.wheels} wheels"


class Car(Vehicle):
    def __init__(self, name, wheels, fuel_type):
        super().__init__(name, wheels)
        self.fuel_type= fuel_type

    def description(self):
        return f"{super().description()}, Fuel: {self.fuel_type}"

class Bike(Vehicle):
    def __init__(self, name, wheels, is_geared):
        super().__init__(name, wheels)
        self.is_geared= is_geared

    def description(self):
        gear_status = "Geared" if self.is_geared else "Non-geared"
        return f"{super().description()}, Type:{gear_status}"

car = Car("Ford Taurus", 4, "Petrol")
bike = Bike("PCJ 600", 2, True)
print(car.description())
print(bike.description())