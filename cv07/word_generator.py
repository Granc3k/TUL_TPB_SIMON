import random


# Generate random words for the text files
def generate_random_words(num_words):
    words = [
        "apple",
        "banana",
        "cherry",
        "date",
        "elephant",
        "fig",
        "grape",
        "honeydew",
        "iguana",
        "jungle",
        "kite",
        "lemon",
        "mango",
        "nectarine",
        "orange",
        "peach",
        "quince",
        "raspberry",
        "strawberry",
        "tangerine",
        "umbrella",
        "violet",
        "watermelon",
        "xylophone",
        "yam",
        "zebra",
    ]
    return " ".join(random.choices(words, k=num_words))


# Create 6 files with random content
file_paths = []
for i in range(6):
    num_words = random.randint(5, 30)  # Random number of words between 5 and 30
    content = generate_random_words(num_words)
    file_path = f"./cv07/data/bonus_not_appended/random_words_{i+1}.txt"
    with open(file_path, "w") as f:
        f.write(content)
    file_paths.append(file_path)

file_paths
