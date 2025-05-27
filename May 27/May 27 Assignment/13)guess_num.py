import random
target = random.randint(1, 100)
print("Guess a number between 1 and 100")
while True:
    guess = int(input("Your guess: "))
    if guess == target:
        print(f"You guessed the right number")
        break
    elif guess < target:
        print("Too low")
    else:
        print("Too high")