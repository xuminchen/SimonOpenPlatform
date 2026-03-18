from subprojects._shared.db import MySQLDatabase


def connect_to_db():
    return MySQLDatabase().connect()
