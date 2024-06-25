import csv
import os

import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.getenv('DATABASE_URL')


def run():
    characters = []
    changes = 0
    with open('cosmeredle.csv') as file:
        reader = csv.DictReader(file)
        for row in reader:
            row_lower = {key.lower().replace(" ", "_"): value for key, value in row.items()}
            characters.append(row_lower)

    conn = psycopg2.connect(DATABASE_URL)

    create_tables(conn)

    for character in characters:
        check_res = check_char(character, conn)
        if check_res == 0:
            add_char(character, conn)
            print("Added character: " + character["name"])
            changes += 1
        elif type(check_res) is list and len(check_res) > 0:
            for key in check_res:
                update_char(character, conn, key)
                print("Updated character: " + character["name"] + " - " + key)
            changes += 1

    conn.close()
    if changes == 0:
        print("No changes made.")


def check_for_table(conn, table):
    cur = conn.cursor()
    cur.execute(
        """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables 
                WHERE table_name = %s
            );
        """, (table,)
    )
    return cur.fetchone()[0]


def create_tables(conn):
    cur = conn.cursor()
    print("Attempting to create characters table")
    if check_for_table(conn, "characters"):
        print("Table 'characters' already exists")
    else:
        cur.execute(
            """
                CREATE TABLE IF NOT EXISTS characters (
                    id SERIAL PRIMARY KEY, 
                     name TEXT NOT NULL UNIQUE, 
                    home_world TEXT NOT NULL, 
                    first_appearance TEXT NOT NULL, 
                    species TEXT NOT NULL, abilities TEXT NOT NULL
                );
            """
        )
        print("Table 'characters' has been created")

    print("Attempting to create prev_chars table")
    if check_for_table(conn, "prev_chars"):
        print("Table 'prev_chars' already exists")
    else:
        cur.execute(
            """
                CREATE TABLE IF NOT EXISTS prev_chars(
                    date DATE NOT NULL UNIQUE, 
                    id INTEGER NOT NULL
                );
            """
        )
        print("Table 'prev_chars' has been created")

    conn.commit()


def check_char(character, conn):
    dict_cur = conn.cursor(cursor_factory=RealDictCursor)
    dict_cur.execute("SELECT * FROM characters WHERE name = %(name)s;", {"name": character["name"]})

    res = dict_cur.fetchone()
    dict_cur.close()

    if res:
        res = dict(res)
    else:
        res = None

    keys = []

    if res is None:
        return 0
    for key in character.keys():
        if res[key] != character[key]:
            keys.append(key)
    return keys


def add_char(character, conn):
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO characters (name, home_world, first_appearance, species, abilities) VALUES (%(name)s, "
        "%(home_world)s, %(first_appearance)s, %(species)s, %(abilities)s)",
        {"name": character["name"], "home_world": character["home_world"], "first_appearance": character["first_appearance"], "species": character["species"],
         "abilities": character["abilities"]})
    cur.close()
    conn.commit()


def update_char(character, conn, key):
    cur = conn.cursor()
    cur.execute("UPDATE characters SET " + key + " = %(val)s WHERE name = %(name)s", {"val": character[key], "name": character["name"]})
    cur.close()
    conn.commit()


run()
