import sqlite3


class StatusStore:
    def _chunks(self, data, rows=10000):
        for i in range(0, len(data), rows):
            yield data[i : i + rows]

    def _batch_select(self, sql, batch_size, f):
        results = list(self.connection.execute(sql))

        for chunk in self._chunks(results, batch_size):
            yield f(chunk)

    def _execute(self, statements):
        self.connection.execute("BEGIN TRANSACTION")

        result = None

        try:
            result = [list(self.connection.execute(sql)) for sql in statements]
        except:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()

        return result

    def _truncate(self):
        sql = f"DELETE FROM {self.table_name}"
        return self._execute([sql])

    def _create_status(self, rows):
        def _create(row):
            return {"bnumber": row[0], "status": row[1], "notes": row[2]}

        return [_create(row) for row in rows]

    def _update_sql(self, bnumber, status, notes):
        return (
            f"UPDATE {self.table_name} SET "
            f"status = '{status}', notes = '{notes}' "
            f"WHERE bnumber = '{bnumber}'"
        )

    def __init__(self, db_location, bnumbers=None, table_name="progress"):
        self.db_location = db_location
        self.table_name = table_name

        self.connection = sqlite3.connect(self.db_location, check_same_thread=False)

        self._bnumber_count = 0

        sql = (
            f"CREATE TABLE IF NOT EXISTS {self.table_name} "
            f"(bnumber text PRIMARY KEY, status text, notes text)"
        )

        self._execute([sql])

        if bnumbers is not None:
            print(f"Initialising with reset!")
            self.reset(bnumbers)

    def count(self):
        sql = f"SELECT COUNT(*) FROM {self.table_name}"

        result = self._execute([sql])

        return result[0][0][0]

    def count_status(self, status):
        sql = f"SELECT COUNT(*) FROM {self.table_name} " f"WHERE status = '{status}'"

        result = self._execute([sql])

        return result[0][0][0]

    def reset(self, bnumbers):
        self._truncate()
        self._bnumber_count = len(bnumbers)

        for chunk in self._chunks(bnumbers):
            statements = []

            for bnumber in chunk:
                sql = (
                    f"INSERT INTO {self.table_name} " f"(bnumber) VALUES ('{bnumber}')"
                )

                statements.append(sql)

            self._execute(statements)

    def get_all(self, batch_size=1000):
        sql = f"SELECT * FROM {self.table_name}"

        return self._batch_select(sql, batch_size, self._create_status)

    def get_status(self, status, batch_size=1000):
        sql = f"SELECT * FROM {self.table_name} " f"WHERE status = '{status}'"

        return self._batch_select(sql, batch_size, self._create_status)

    def get(self, bnumber):
        sql = f"SELECT * FROM {self.table_name} " f"WHERE bnumber = '{bnumber}'"

        result = list(self._batch_select(sql, 1, self._create_status))

        if result and result[0]:
            return result[0][0]
        else:
            return None

    def update(self, bnumber, status, notes=""):
        status = str(status).strip()
        sql = self._update_sql(bnumber, status, notes)

        self._execute([sql])

    def batch_update(self, status_updates):
        chunks = self._chunks(status_updates)

        for chunk in chunks:
            statements = []

            for status_update in status_updates:
                bnumber = status_update["bnumber"]
                status = status_update["status"]
                notes = status_update.get("notes", "")

                sql = self._update_sql(bnumber, status, notes)

                statements.append(sql)

            self._execute(statements)
