import psycopg2
import os
import dotenv

dotenv.load_dotenv()

def task_4():
    conn = None
    view_command_1 = """
                    CREATE OR REPLACE VIEW task_4
                    AS SELECT student.name AS Student,
                            mentor.name AS Mentor
                    FROM student
                    JOIN mentor
                    ON student.mentor_id = mentor.id
                    WHERE student.city = 'Berlin'
                    OR mentor.city = 'Berlin'
                    ORDER BY student.name;
                    """
    join_command = """
                    SELECT task_2_1.Topic,
                        task_4.Mentor,
                        task_4.Student
                    FROM task_2_1
                    JOIN task_4
                    ON task_2_1.Mentor = task_4.Mentor
                    ORDER BY task_2_1.Topic;
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
            cursor.execute('''SELECT table_name 
                            AS view_name
                            FROM information_schema.views
                            WHERE table_schema = 'public'
                            AND table_name = %s
                            ;'''
                            , (view_name,))
            return cursor.fetchone() is not None
        
        if not view_exists('task_4'):
            cursor.execute(view_command_1)
        
        cursor.execute(join_command)
        
        rows = cursor.fetchall()
        
        print("{:<25} | {:<25} | {:<25}".format("Topic", "Mentor", "Student"))
        print('-' * 80)
        for row in rows:
            print("{:<25} | {:<25} | {:<25}".format(*(str(value) if value is not None else '' for value in row)))
        
    except(Exception, psycopg2.DatabaseError) as error:
        raise error
    finally:
        if conn is not None:
            conn.close()
            
if __name__=='__main__':
    task_4()