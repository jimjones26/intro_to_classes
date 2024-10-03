"""
Introduction to classes and object-oriented programming in python
"""


# ------------------------------------------------------------------
# define a class
# ------------------------------------------------------------------
class Car:
    # Class attribute to keep track of total cars produced
    total_cars_produced = 0

    # Constructor method to initialize the object
    def __init__(self, make, model, year):
        self.make = make  # Instance variable for the car's make
        self.model = model  # Instance variable for the car's model
        self.year = year  # Instance variable for the car's year
        Car.total_cars_produced += (
            1  #  Increment total cars produced each time  a new car is created
        )

    # Method to display car details
    def display_details(self):
        print(f"Car: {self.year} {self.make} {self.model}")

    # Method to update the car's year
    def update_year(self, new_year):
        self.year = new_year
        print(f"Year updated to {self.year}")


# ------------------------------------------------------------------
# Create an instance of the class
# ------------------------------------------------------------------
my_car = Car("Ferarri", "F8 Tributo", 2020)
another_car = Car("Lamborghini", "Aventador", 2019)

# ------------------------------------------------------------------
# Access the attributes of the instance
# ------------------------------------------------------------------
my_car.model
my_car.year
my_car.make

# ------------------------------------------------------------------
# Call a method on the instance
# ------------------------------------------------------------------
my_car.display_details()

# ------------------------------------------------------------------
# Access the class attribute for total number of cars produced
# ------------------------------------------------------------------
print(f"Total cars produced: {Car.total_cars_produced}")
