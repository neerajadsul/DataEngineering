import random


def ask_for_input():
    """Asks and validates user input for a single alphabet."""
    while True:
        guess = input("Guess a letter: ")

        if len(guess) == 1 and guess.isalpha():
            break
        else:
            print('Invalid letter. Please enter a single alphabet.')


word_list = ['Raspberry', 'Strawberry', 'Apple', 'Blueberry', 'Avocado']
word = random.choice(word_list).lower()
