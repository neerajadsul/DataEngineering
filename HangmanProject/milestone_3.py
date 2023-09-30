import random


def check_guess(guess, word):
    """Returns True if guess is in the word, else False."""
    if guess.lower() in word:
        print(f'Good guess! {guess} is in the word.')
        response = True
    else:
        print(f'Sorry, {guess} is not in the word. Try again.')
        response = False

    return response


def ask_for_input():
    """Asks and validates user input for a single alphabet."""
    while True:
        guess = input("Guess a letter: ")

        if len(guess) == 1 and guess.isalpha():
            return guess
        else:
            print('Invalid letter. Please enter a single alphabet.')


if __name__ == "__main__":
    word_list = ['Raspberry', 'Strawberry', 'Apple', 'Blueberry', 'Avocado']
    word = random.choice(word_list).lower()

    guess = ask_for_input()
    check_guess(guess, word)
