import psycopg2
import os
import dotenv

dotenv.load_dotenv()


def task_2():
    conn = None
    view_command_1 = """
                    CREATE VIEW task_2_1
                    AS SELECT module.name AS Topic,
                            mentor.name AS Mentor
                    FROM module
                    LEFT JOIN mentor 
                    ON mentor.id = module.teacher;
                    """
    view_command_2 = """
                    CREATE VIEW task_2_2
                    AS SELECT student_and_mentor.Student,
                            task_2_1.Topic,
                            task_2_1.Mentor
                    FROM student_and_mentor
                    LEFT JOIN task_2_1
                    ON student_and_mentor.Mentor = task_2_1.Mentor;
                    """
    try:
        
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('PASSWORD'),
            port=os.getenv('DB_PORT')
        )
        
        cursor = conn.cursor()
        
        def view_exists(view_name):
            cursor.execute("""
                        SELECT table_name
                        AS view_name
                        FROM information_schema.views
                        WHERE table_schema = 'public'
                        AND table_name = %s""", (view_name,))
            return cursor.fetchone() is not None
        
        if not view_exists('task_2_1'):
            cursor.execute(view_command_1)
        if not view_exists('task_2_2'):
            cursor.execute(view_command_2)
        
        cursor.execute('SELECT * FROM task_2_2;')
        
        rows = cursor.fetchall()
        
        conn.commit()
        cursor.close()
        
        print("{:<20} | {:<30} | {:<20}".format("Student", "Topic", "Mentor"))
        print('-' * 75)
        for row in rows:
            print("{:<20} | {:<30} | {:<20}".format(*(str(value) if value is not None else '' for value in row)))
        
    except(Exception, psycopg2.DatabaseError) as error:
        raise error
    finally:
        if conn is not None:
            conn.close()




if __name__=='__main__':
    task_2()