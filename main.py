import sqlalchemy as sq
from sqlalchemy.orm import sessionmaker
import json
import os
from models import Base, Publisher, Shop, Book, Stock, Sale


class BookSalesDB:
    def __init__(self, driver, user, password, host, port, name_db):
        self.DSN = f'{driver}://{user}:{password}@{host}:{port}/{name_db}'
        self.engine = sq.create_engine(self.DSN)

        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        self.init_db()

    def init_db(self):
        from sqlalchemy_utils import database_exists, create_database
        if not database_exists(self.engine.url):
            create_database(self.engine.url)

        # Base.metadata.drop_all(engine)
        Base.metadata.create_all(self.engine)

    def fill_data(self, file_name):
        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)

        cwd = os.getcwd()
        full_file_name = os.path.join(cwd, "fixtures", file_name)
        with open(full_file_name, "r", encoding="utf-8") as read_file:
            ini_data = json.load(read_file)

        rows = []
        models = {
            'publisher': Publisher,
            'shop': Shop,
            'book': Book,
            'stock': Stock,
            'sale': Sale}

        for key in ini_data.keys():
            for record in ini_data[key]:
                rows.append(models[key](**record))

        self.session.add_all(rows)
        self.session.commit()

    def search_sales(self, search_data):
        if search_data.isdigit():
            q_publisher = self.session.query(Publisher).filter(Publisher.id == int(search_data)).subquery()
        else:
            q_publisher = self.session.query(Publisher).filter(Publisher.name.ilike(f'%{search_data}%')).subquery()

        q = self.session.query(Stock).\
                join(Book, Book.id == Stock.id_book). \
                join(q_publisher, Book.id_publisher == q_publisher.c.id)

        result = []
        for s in q.all():
            for sale in s.sales:
                result.append({
                    'book_title': s.books.title,
                    'shop_name': s.shops.name,
                    'price': sale.price,
                    'date_sale': sale.date_sale
                })
                # print("\t", s.books.title, s.shops.name, sale.price, sale.date_sale)
        return result


if __name__ == '__main__':
    bs = BookSalesDB('postgresql+psycopg2', 'postgres', '0000', 'localhost', '5432', 'book_sale')
    bs.fill_data("data_file.json")

    print("""
fill_db - fill the database with data from the file 'data_file.json'
exit - exit program
any other string - search sales  
    """)

    while True:
        command = input('Input command: ')
        if command=='fill_db':
            bs.fill_data("data_file.json")
        elif command=='exit':
            break
        else:
            rs = bs.search_sales(command)
            if len(rs) == 0:
                print('nothing found')
            else:
                for record in rs:
                    print("\t", " | ".join(str(v) for v in record.values()))

    bs.session.close()
