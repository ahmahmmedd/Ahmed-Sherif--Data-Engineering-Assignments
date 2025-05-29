def is_prime(n):
    if n< 2:
        return False
    for i in range(2,n):
        if n%i== 0:
            return False
    return True

for num in range(1, 101):
    if is_prime(num):
        print(num, end=", ")
