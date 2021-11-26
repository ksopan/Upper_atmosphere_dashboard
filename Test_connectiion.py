def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn
        
def main():
    database = "bk_space.db"

    # create a database connection
    conn = create_connection(database)
    with conn:
        