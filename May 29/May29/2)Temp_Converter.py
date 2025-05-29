def convert_temp(value, unit):
    if unit== 'C':
        return (value*9/5)+ 32
    elif unit== 'F':
        return (value-32)* 5/9

print(convert_temp(10, 'C'))
print(convert_temp(32, 'F'))