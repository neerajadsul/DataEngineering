import random


class Hangman:
    """Wordgame Hangman"""
    def __init__(self, wordlist, numlives=5):
        """Create initial state of the game."""
        self.wordlist = wordlist
        self.numlives = numlives
        self.word = random.choice(wordlist).lower()
        self.word_guessed = ['_']*len(self.word)
        self.guesses = []

    def check_guess(self, guess):
        """Returns True if guess is in the word, else False."""
        guess = guess.lower()
        if guess in self.word:
            print(f'Good guess! {guess} is in the word.')
        else:
            print(f'Sorry, {guess} is not in the word. Try again.')

    def ask_for_input(self):
        """Asks and validates user input for a single alphabet."""
        print(self.word_guessed)
        print(f'{self.numlives} lives left.')
        guess = input("Guess a letter: ")
        if not (len(guess) == 1 and guess.isalpha()):
            print('Invalid letter. Please enter a single alphabet.')
        elif guess in self.guesses:
            print('Already tried that letter.')
        else:
            self.check_guess(guess)


if __name__ == '__main__':
    wordlist = ['Raspberry', 'Strawberry', 'Apple', 'Blueberry', 'Avocado']
    game = Hangman(wordlist)
