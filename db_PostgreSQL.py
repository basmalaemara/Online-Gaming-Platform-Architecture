import psycopg2

def get_postgres_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="monster_arena",  # <- change this to your actual DB name
        user="basmalayasser",
        password="Abeersherif2004"
    )

