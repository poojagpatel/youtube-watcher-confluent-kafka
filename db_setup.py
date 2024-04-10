import psycopg2
from config import config

db_config = config['db_config']


def create_table(conn, cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS youtube_stats (
            video_id VARCHAR(255),
            title VARCHAR(500),
            views INT,
            likes INT,
            comments INT,
            timestamp TIMESTAMP
        )
    """)
    conn.commit()


if __name__ == '__main__':
    try:
        conn = psycopg2.connect(host= db_config['host'], dbname=db_config['dbname'], user=db_config['user'], password=db_config['password'])
        cur = conn.cursor()
        create_table(conn, cur)
    except Exception as e:
        print(e)
