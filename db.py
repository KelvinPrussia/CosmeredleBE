import psycopg2
import os
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')

conn_pool = psycopg2.pool.SimpleConnectionPool(1,
                                               97,
                                               dsn=DATABASE_URL,
                                               sslmode="disable")


if conn_pool:
    print("Successfully created connection pool")


def get_db_connection():
    try:
        conn = conn_pool.getconn()
        if conn:
            print("Successfully received a connection from the pool")
            return conn
    except Exception as error:
        print("Error while connecting to PostgreSQL", error)


def release_db_connection(connection):
    try:
        conn_pool.putconn(connection)
        print("Successfully returned connection to the connection pool")
    except Exception as error:
        print("Error while returning connection to the pool", error)


def get_names():
    conn = None
    try:
        conn = get_db_connection()
        if conn is None:
            raise Exception("Failed to get a connection from the pool")

        cur = conn.cursor()
        cur.execute("SELECT name FROM characters;")
        names = cur.fetchall()
        cur.close()
        names_list = [tup[0] for tup in names]
        print("DB: Returned character names")
        return names_list

    except Exception as error:
        print(f"Error: {error}")
        return []
    finally:
        if conn:
            release_db_connection(conn)


def get_character_by_name(name):
    conn = None
    try:
        conn = get_db_connection()
        if conn is None:
            raise Exception("Failed to get a connection from the pool")

        dict_cur = conn.cursor(cursor_factory=RealDictCursor)
        dict_cur.execute("SELECT * FROM characters WHERE name = %(name)s", {"name": name})
        res = dict_cur.fetchone()
        if res:
            del res["id"]

        dict_cur.close()
        print("DB: Returned character - ", res["name"])
        return res
    except Exception as error:
        print(f"Error: {error}")
    finally:
        if conn:
            release_db_connection(conn)


def get_character_count():
    conn = None
    try:
        conn = get_db_connection()
        if conn is None:
            raise Exception("Failed to get a connection from the pool")

        cur = conn.cursor()
        cur.execute("SELECT COUNT(name) FROM characters")
        res = cur.fetchone()

        cur.close()
        print("DB: Returned character count")
        return res[0]
    except Exception as error:
        print(f"Error: {error}")
    finally:
        if conn:
            release_db_connection(conn)


def get_character_by_id(id):
    conn = None
    try:
        conn = get_db_connection()
        if conn is None:
            raise Exception("Failed to get a connection from the pool")

        dict_cur = conn.cursor(cursor_factory=RealDictCursor)
        dict_cur.execute("SELECT * FROM characters WHERE id = %(id)s", {"id": id})
        res = dict_cur.fetchone()
        if res:
            del res["id"]

        dict_cur.close()
        print("DB: Returned character - ", res["name"])
        return res
    except Exception as error:
        print(f"Error: {error}")
    finally:
        if conn:
            release_db_connection(conn)


def get_prev_char_by_id(id):
    conn = None
    try:
        conn = get_db_connection()
        if conn is None:
            raise Exception("Failed to get a connection from the pool")

        cur = conn.cursor()
        cur.execute("SELECT id FROM prev_chars WHERE id = %(id)s", {"id": id})
        res = cur.fetchone()

        cur.close()
        print("DB: Checked if prev_char", id, "exists")
        return res is not None
    except Exception as error:
        print(f"Error: {error}")
    finally:
        if conn:
            release_db_connection(conn)


def get_prev_char_by_date(date):
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("SELECT id FROM prev_chars WHERE date = %(date)s", {"date": date})
        res = cur.fetchone()

        cur.close()
        if res is not None:
            print("DB: Returned previous character")
            return res[0]
        else:
            print("DB: No previous character to return")
            return None
    except Exception as error:
        print(f"Error: {error}")
    finally:
        if conn:
            release_db_connection(conn)


def insert_to_prev_chars(id, date):
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("INSERT INTO prev_chars (id, date) VALUES(%(id)s, %(date)s)", {"id": id, "date": date})
        conn.commit()
        count = cur.rowcount
        print(count, "Record inserted in prev_chars successfully")
        cur.close()
    except Exception as error:
        print(f"Error: {error}")
    finally:
        if conn:
            release_db_connection(conn)
