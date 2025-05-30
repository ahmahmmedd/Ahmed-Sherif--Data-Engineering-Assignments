def count_words(filename):
    with open(filename, 'r') as f:
        content = f.read()
        return len(content.split())
print("Word count:", count_words('emp.txt'))