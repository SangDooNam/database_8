import psycopg2
import os
import dotenv

dotenv.load_dotenv()


def task_1():
    conn = None
    command_view1 = """
                    CREATE VIEW student_and_mentor 
                    AS SELECT student.name AS Student,
                            mentor.name AS Mentor
                    FROM student
                    LEFT JOIN mentor
                    ON student.mentor_id = mentor.id
                    ORDER BY student.name;
                    """
    command_view2 = """
                    CREATE VIEW student_and_local
                    AS SELECT student.name AS Student,
                                mentor.name AS Mentor
                    FROM student
                    LEFT JOIN mentor
                    ON student.local_mentor = mentor.id
                    ORDER BY student.name;
                    """
    join_command = """
                    SELECT student_and_mentor.Student AS Student,
                        student_and_mentor.Mentor AS Mentor,
                        student_and_local.Mentor AS Local_mentor
                    FROM student_and_mentor
                    LEFT JOIN student_and_local 
                    ON student_and_mentor.Student = student_and_local.Student
                    ORDER BY student_and_mentor.Student;
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
            query = """
                    SELECT table_name AS view_name 
                    FROM information_schema.views
                    WHERE table_schema = 'public'
                    AND table_name = %s;
                    """
            cursor.execute(query, (view_name,))
            return cursor.fetchone() is not None
        
        if not view_exists('student_and_mentor'):
            cursor.execute(command_view1)
        if not view_exists('student_and_local'):
            cursor.execute(command_view2)
        cursor.execute(join_command)
        
        rows = cursor.fetchall()
        
        conn.commit()
        
        print("{:<20} | {:<20} | {:<20}".format("Student","Mentor","Local mentor"))
        print('-' * 65)
        for row in rows:
            print("{:<20} | {:<20} | {:<20}".format(*(str(value) if value is not None else '' for value in row)))
        
        cursor.close()

    except(Exception, psycopg2.DatabaseError) as error:
        raise error
    
    finally:
        if conn is not None:
            conn.close()
            


if __name__ == '__main__':
    task_1()