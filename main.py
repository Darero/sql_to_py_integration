import psycopg2
from config import password, host, db_name, user


class Database:
    def __init__(self):
        print('[INFO] Connecting to database')
        try:
            self.connection = psycopg2.connect(host=host, user=user, password=password, database=db_name)
        except Exception as e:
            print(f'[ERROR] {e}')

    def close_db(self):
        self.connection.close()


class DatabaseTable:
    def __init__(self, database, table_name):
        self.connection = database.connection
        self.table_name = table_name

    def execute_sql_script(self, script):
        with self.connection.cursor() as cursor:
            cursor.execute(script)
            return cursor.fetchall()

    def get_column_names(self):
        with self.connection.cursor() as cursor:
            cursor.execute(f'SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = \'{self.table_name}\'')
            columns = [i[3] for i in cursor.fetchall()]
            return columns

    def get_table_data_raw(self):
        print(f'[INFO] Retrieving data from {self.table_name}')
        script = f'Select * from {self.table_name}'
        return self.execute_sql_script(script)

    def get_table_data_raw_by_columns(self, columns: list):
        print(f'[INFO] Retrieving data from {self.table_name}')
        if all(map(lambda x: type(x) is str, columns)):
            script = f'SELECT {",".join(columns)} from {self.table_name}'
            return self.execute_sql_script(script)

    def get_data_raw_inner_join(self, join_table_name, key):
        print(f'[INFO] Retrieving data from {self.table_name} and {join_table_name}')
        script = f'SELECT * from {self.table_name} ' \
                 f'INNER JOIN {join_table_name} USING({key})'
        return self.execute_sql_script(script)


db = Database()
author = DatabaseTable(db, 'author')
print(author.get_table_data_raw())

# [(1, 'Булгаков М.А.'), (2, 'Достоевский Ф.М.'), (3, 'Есенин С.А.'), (4, 'Пастернак Б.Л.')]

print(author.get_column_names())

# ['author_id', 'name_author']

book = DatabaseTable(db, 'book')
for i in book.get_table_data_raw_by_columns(['title', 'price', 'amount']):
    print(i)

# ('Мастер и Маргарита', Decimal('670.99'), 3)
# ('Белая гвардия ', Decimal('540.50'), 5)
# ('Идиот', Decimal('460.00'), 10)
# ('Братья Карамазовы', Decimal('799.01'), 3)
# ('Игрок', Decimal('480.50'), 10)
# ('Стихотворения и поэмы', Decimal('650.00'), 15)
# ('Черный человек', Decimal('570.20'), 6)
# ('Лирика', Decimal('518.99'), 2)

genre = DatabaseTable(db, 'genre')
for i in genre.get_data_raw_inner_join('book', 'genre_id'):
    print(i)

# (1, 'Роман', 1, 'Мастер и Маргарита', 1, Decimal('670.99'), 3)
# (1, 'Роман', 2, 'Белая гвардия ', 1, Decimal('540.50'), 5)
# (1, 'Роман', 3, 'Идиот', 2, Decimal('460.00'), 10)
# (1, 'Роман', 4, 'Братья Карамазовы', 2, Decimal('799.01'), 3)
# (1, 'Роман', 5, 'Игрок', 2, Decimal('480.50'), 10)
# (2, 'Поэзия', 6, 'Стихотворения и поэмы', 3, Decimal('650.00'), 15)
# (2, 'Поэзия', 7, 'Черный человек', 3, Decimal('570.20'), 6)
# (2, 'Поэзия', 8, 'Лирика', 4, Decimal('518.99'), 2)

db.close_db()
