import psycopg2
import psycopg2.extras


class PostgresConnectorContextManager:
    def __init__(self, db_host: str, db_name: str, db_user: str, db_password: str, db_port: int):
        self.db_host = db_host
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_port = db_port
        self.conn = None

    def __enter__(self):
        self.conn = psycopg2.connect(
            host=self.db_host,
            database=self.db_name,
            user=self.db_user,
            password=self.db_password,
            port=self.db_port
        )
        return self.conn  # return the raw connection directly

    def __exit__(self, exc_type, exc_value, traceback):
        if self.conn:
            self.conn.close()

    def get_data_sql(self, query: str):
        """
        Executes a SQL query and returns results as a list of dicts.
        """
        with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(query)
            rows = cur.fetchall()
            return [dict(row) for row in rows]
