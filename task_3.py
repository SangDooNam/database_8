import psycopg2
import os
import dotenv

dotenv.load_dotenv()


def task_3():
    conn = None
    command = """
            SELECT task_2_1.Topic, 
            task_2_1.Mentor, 
            student_and_mentor.Student
            FROM task_2_1
            LEFT JOIN student_and_mentor
            ON student_and_mentor.Mentor = task_2_1.Mentor
            ORDER BY task_2_1.topic
            ;"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('PASSWORD'),
            port=os.getenv('DB_PORT')
        )

        cursor = conn.cursor()
        
        cursor.execute(command)
        
        rows = cursor.fetchall()
        
        conn.commit()
        cursor.close()
        
        print("{:<30} | {:<30} | {:<30}".format("Topic", "Mentor", "Student"))
        print('-' * 90)
        for row in rows:
            print("{:<30} | {:<30} | {:<30}".format(*(str(value) if value is not None else '' for value in row)))
        
    except(Exception, psycopg2.DatabaseError) as error:
        raise error
        
    finally:
        if conn is not None:
            conn.close()

if __name__ =='__main__':
    task_3()