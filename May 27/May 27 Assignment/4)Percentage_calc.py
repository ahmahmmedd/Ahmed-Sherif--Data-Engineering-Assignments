subjects= ["English", "Data Science", "Arabic", "Social Studies", "Physics"]
marks= []
tt=0
for i in subjects:
    mark= int(input(f"Enter marks for {i}: "))
    marks.append(mark)
    tt+= mark
avg = tt/len(subjects)
percentage = (tt/(100 * len(subjects)))*100
print(f"Total: {tt}")
print(f"Average marks: {avg}")
print(f"Percentage: {percentage}%")

if percentage >= 90:
    print("Grade: A")
elif percentage >= 75:
    print("Grade: B")
elif percentage >= 60:
    print("Grade: C")
elif percentage >= 50:
    print("Grade: D")
else:
    print("Grade: F")