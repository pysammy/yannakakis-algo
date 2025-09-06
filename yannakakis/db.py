import psycopg2

class Database():
    def __init__(self):
        self.db_config = {
        "dbname": "imdb",
        "user": "postgres",
        "password": "1234",
        "host": "localhost",
        "port": "5432", 
    }
    
        self.connection = psycopg2.connect(**self.db_config) # Connect to DB

    def fetch_table_from_db(self, table_name, column_names, connection):
        query = f"SELECT {', '.join(column_names)} FROM {table_name};"
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()

        # Convert to list of dictionaries
        return [dict(zip(column_names, row)) for row in rows]
    
    def closeConnection(self):
        if self.connection:
            self.connection.close()

