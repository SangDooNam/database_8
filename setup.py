import psycopg2
import os
import dotenv

dotenv.load_dotenv()

class TableNameError(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)

def setup(table_commands, 
        insert_mentor,
        insert_student,
        mentor_data,
        student_data,
        alter_table_commands,
        update_command):
    
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
        for command in table_commands:
            cursor.execute(command)
            
        def exists(data, table):
            if table not in {'mentor', 'student'}:
                raise TableNameError("Invalid table name")
            query = f'SELECT id FROM {table} WHERE id = %s'
            cursor.execute(query, (data[0],))
            return cursor.fetchone() is not None
        
        for value in mentor_data:
            if not exists(value, 'mentor'):
                cursor.execute(insert_mentor, (value[1], value[2]))
        
        for value in student_data:
            if not exists(value, 'student'):
                cursor.execute(insert_student, (value[1], value[2], value[3]))
        
        def column_exists(table_name, column_name):
            cursor.execute('''SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = %s 
                        AND column_name = %s''', 
                        (table_name, column_name))
            return cursor.fetchone() is not None
        
        if not column_exists('student', 'local_mentor'):
            for command in alter_table_commands:
                cursor.execute(command)
        
        cursor.execute(update_command)
        
        conn.commit()
        cursor.close()
        
    except(Exception, psycopg2.DatabaseError) as error:
        raise error
        
    finally:
        if conn is not None:
            conn.close()



table_commands =[
            """
            CREATE TABLE IF NOT EXISTS mentor (
                id SERIAL PRIMARY KEY,
                name VARCHAR(25),
                city VARCHAR(25)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS student (
                id SERIAL PRIMARY KEY,
                name VARCHAR(25),
                city VARCHAR(25),
                mentor_id INT REFERENCES mentor(id)
            );
            """
            ]

insert_mentor=["""INSERT INTO mentor (
        name, city)
        VALUES(%s, %s);"""],
insert_student=["""INSERT INTO student(
        name, city, mentor_id)
        VALUES(%s, %s, %s);"""]


student_data = [
    (1, 'Dolores Perez', 'Barcelona', 2),
    (2, 'Maria Highsmith', 'New York', 3),
    (3, 'Aimaar Abdul', 'Chicago', 1),
    (4, 'Gudrun Schmidt', 'Berlin', 5),
    (5, 'Gerald Hutticher', 'Berlin', 6),
    (6, 'Itzi Elizabal', 'Barcelona', 4),
    (7, 'Irmgard Seekircher', 'Berlin', 7),
    (8, 'Christian Blanc', 'Paris', 4),
    (9, 'Alex Anjou', 'Paris', 3),
    (10, 'John Goldwin', 'Chicago', 6),
    (11, 'Emilio Ramiro', 'Barcelona'),
    (12, 'Wayne Green', 'New York')
]


mentor_data = [
    (1, 'Peter Smith', 'New York'),
    (2, 'Laura Wild', 'Chicago'),
    (3, 'Julius Maxim', 'Berlin'),
    (4, 'Melinda O''Connor', 'Berlin'),
    (5, 'Patricia Boulard', 'Marseille'),
    (6, 'Julia Vila', 'Barcelona'),
    (7, 'Fabienne Martin', 'Paris'),
    (8, 'Rose Dupond', 'Brussels'),
    (9, 'Ahmed Ali', 'Marseille')
]

alter_table_commands = [
                        """ALTER TABLE student ADD COLUMN local_mentor integer;""",
                        """ALTER TABLE student ADD CONSTRAINT student_local_mentor_fkey FOREIGN KEY (local_mentor) REFERENCES mentor(id);"""
                        ]

update_command = """UPDATE student
                    SET local_mentor = mentor.id
                    FROM mentor
                    WHERE mentor.city = student.city;
                    """


if __name__ == '__main__':
    setup(table_commands, 
        insert_mentor,
        insert_student,
        mentor_data,
        student_data,
        alter_table_commands,
        update_command)