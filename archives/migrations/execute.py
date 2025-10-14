import os
import psycopg2

DB_PARAMS = {
    'dbname': os.environ['POSTGRES_DB'],
    'user': os.environ['POSTGRES_USER'],
    'password': os.environ['POSTGRES_PASSWORD'],
    'host': os.environ['POSTGRES_HOST'],
    'port': os.environ['POSTGRES_PORT']
}

CREATE_MIGRATIONS_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS migrations (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    applied BOOLEAN NOT NULL DEFAULT FALSE
);
"""

def create_migrations_table():
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    try:
        cur.execute(CREATE_MIGRATIONS_TABLE_QUERY)
        conn.commit()
    except Exception as e:
        print(f"Error creating migrations table: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


def get_applied_migrations():
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    try:
        cur.execute("SELECT name FROM migrations WHERE applied = TRUE")
        applied_migrations = cur.fetchall()
        return [migration[0] for migration in applied_migrations]
    except Exception as e:
        print(f"Error fetching applied migrations: {e}")
        return []
    finally:
        cur.close()
        conn.close()


def apply_migration(migration_name, migration_path):
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    try:
        exec(open(migration_path).read())

        cur.execute(
            "INSERT INTO migrations (name, applied) VALUES (%s, %s) ON CONFLICT (name) DO UPDATE SET applied = EXCLUDED.applied",
            (migration_path, True))
        conn.commit()
        print(f"Migration {migration_name} applied successfully.")
    except Exception as e:
        print(f"Error applying migration {migration_name}: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


def run_migrations():
    create_migrations_table()
    applied_migrations = get_applied_migrations()

    migration_directories = ['migrations/es', 'migrations/postgres']
    for directory in migration_directories:
        for filename in os.listdir(directory):
            migration_path = os.path.join(directory, filename)
            if filename.endswith('.py') and migration_path not in applied_migrations:
                apply_migration(filename, migration_path)


if __name__ == "__main__":
    run_migrations()