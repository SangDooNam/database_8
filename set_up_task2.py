import psycopg2
import os
import dotenv

dotenv.load_dotenv()

def setup(command_table, command_insert, data):
    conn = None
    
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('PASSWORD'),
            port=os.getenv('DB_PORT')
        )
        cursor = conn.cursor()
        
        def exists(value):
            cursor.execute('SELECT id FROM module WHERE id = %s', (value[0],))
            return cursor.fetchone() is not None
        
        cursor.execute(command_table)
        
        for value in data:
            if not exists(value):
                cursor.execute(command_insert, (value[1], value[2]))
        
        conn.commit()
        cursor.close()
        
    except (Exception, psycopg2.DatabaseError) as error:
        raise error
    finally:
        if conn is not None:
            conn.close()


command_table = """
                CREATE TABLE IF NOT EXISTS module (
                id serial PRIMARY KEY,
                name varchar(100),
                teacher integer,
                FOREIGN KEY (teacher) REFERENCES mentor(id)
                );"""

command_insert = """
                INSERT INTO module(name, teacher) 
                VALUES (%s, %s)            
                """
insert_data = [
    (1, 'Computer Basics', 4),
    (2, 'Python Basics', 5),
    (3, 'Python Algorithms', 1),
    (4, 'Python Data Structures', 6),
    (5, 'Python Web Frameworks', 2),
    (6, 'Database Basics', 9),
    (7, 'SQL', 7),
    (8, 'Advanced SQL', 3),
    (9, 'Django Basics', 2),
    (10, 'Django Admin', 8),
    (11, 'Django ORM', None),
    (12, 'Frontend', 2)
]



if __name__ == '__main__':
    setup(command_table, command_insert, insert_data)