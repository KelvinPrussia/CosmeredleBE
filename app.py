import random
from datetime import datetime, UTC

from flask import Flask, jsonify
from flask_restful import Resource, Api
from flask_cors import CORS

import db
import parser

app = Flask('CosmeredleAPI')
CORS(app)
api = Api(app)

# load list of names/attributes
character_names = db.get_names()
global correct_char


class CharacterList(Resource):
    def get(self):
        return jsonify(names=character_names)


class GuessResult(Resource):
    def get(self, character_name):
        guess_char = db.get_character_by_name(character_name)
        return check_matches(guess_char, correct_char)


class CorrectChar(Resource):
    def get(self):
        character_data = correct_char
        return character_data


class ResetCorrectChar(Resource):
    def post(self):
        set_correct_char()


api.add_resource(CharacterList, "/list")
api.add_resource(GuessResult, "/guess/<character_name>")
api.add_resource(CorrectChar, "/correctchar")
api.add_resource(ResetCorrectChar, "/resetchar")


def check_matches(guess, correct):
    guess_result = {"name": [guess["name"], 2],
                    "home_world": [guess["home_world"], 2],
                    "first_appearance": [guess["first_appearance"], 2],
                    "species": [guess["species"], 2],
                    "abilities": [guess["abilities"], 2]}

    # Check if names match, if yes then return true for all matches
    if guess["name"].lower() == correct["name"].lower():
        return guess_result

    for attribute in correct:
        match attribute:
            case "species":
                guess_species = guess[attribute].lower()
                correct_species = correct[attribute].lower()
                if guess_species == correct_species:
                    continue
                elif "(" in guess_species and correct_species:
                    guess_result[attribute][1] = compare_prefix(guess_species, correct_species)

            case "abilities":
                guess_result[attribute][1] = compare_lists(guess[attribute].lower(), correct[attribute].lower())

            case _:
                if guess[attribute].lower() == correct[attribute].lower():
                    continue
                else:
                    guess_result[attribute][1] = 0
    return guess_result


def compare_prefix(guess_species, correct_species):
    if guess_species.split("(")[0] == correct_species.split("(")[0]:
        return 1
    else:
        return 0


def compare_lists(guess_list, correct_list):
    guess_list = guess_list.split(", ")
    correct_list = correct_list.split(", ")
    if sorted(guess_list) == sorted(correct_list):
        return 2
    elif set(guess_list) & set(correct_list):
        return 1
    else:
        return 0


def set_correct_char():
    print("Attempting to set correct char")
    global correct_char
    unused_ids = db.get_unused_ids()

    rand_num = random.randrange(0, len(unused_ids) - 1)
    rand_id = unused_ids[rand_num]
    date = datetime.now(UTC).date()
    daily_char = db.get_prev_char_by_date(date)

    if daily_char:
        print("char already set for ", date)
        correct_char = db.get_character_by_id(daily_char)
        return
    else:
        char = db.get_character_by_id(rand_id)
        correct_char = char
        print("new char set - ", dict(correct_char))
        db.insert_to_prev_chars(rand_id, date)


set_correct_char()

if __name__ == "__main__":
    app.run()
