# Game of words - Hangman

## Description
This directory contains a Python based program to play classic wordgame Hangman via a Terminal interface.
## How to Install and Run?
You will need Python 3 in your system path to be able to run and play this game.

1. Clone the repository and change your working directory on Terminal to `HangmanProject`. Alternatively, you can download or copy-paste the script `milestone_5.py` on your computer and change working directory to the path on Terminal.
2. Run the program by `python3 milestone_5.py`

## Gameplay / Usage
Initially, the game will display a word to guess with all characters hidden and the word length.
You will need to enter your guess by typing an English alphabet and press Enter.
You are allowed 5 incorrect guesses indicated by number of lives. Invalid and duplicate inputs are not counted as incorrect guesses.

If the character is in the word, program will reveal it in its one or more positions. If it is not in the word you loose 1 life.

## Project Organization
The project is implemented as a Python `class Hangman`.
The class is instantiated to create initial state of the game.

## License
See the LICENSE file in the root of the repository.