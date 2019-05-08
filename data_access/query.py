from sqlalchemy.engine.url import URL
from sqlalchemy import MetaData, create_engine, Column, Table, or_
from sqlalchemy.sql import select
from pandas import DataFrame
from pandas.io.common import EmptyDataError
from sqlalchemy.dialects.postgresql.dml import insert

#========================================== Postgresql implementation ==============================================

db_config = {'drivername': 'postgres',
             'username': 'postgres',
             'password': 'H0rAT10',
             'host': '172.20.20.31',
             'port': 5432}

def as_list(x):
    if isinstance(x, str) or isinstance(x, dict):
        return [x]
    elif isinstance(x, tuple):
        return list(x)
    else:
        return [x]

def convert_data_format_for_upsert(data):
    if isinstance(data, list):
        return data
    elif isinstance(data, DataFrame):
        return data.to_dict(orient='records')
    else:
        raise TypeError

def get_columns_info(column_names: [], column_types: [], primary_key_columns: []):
    columns_info = {}
    for column, tp in zip(column_names, column_types):
        columns_info[column] = {}
        columns_info[column]['type_'] = tp
        if column in primary_key_columns:
            columns_info[column]['primary_key'] = True
    return columns_info


def get_column_list(columns_info: {}):
    column_obj = []
    for column in columns_info:
        column_obj.append(Column(column, **columns_info[column]))
    return column_obj


class DatabaseQuery(object):

    def __init__(self, drivername, username, password, host, port=5432):
        self.db_config = {
            'drivername': drivername,
            'username': username,
            'password': password,
            'host': host,
            'port': port
        }
        self.engine, self.meta = self.connect_postgres_db()

    def connect_postgres_db(self):
        url = URL(**self.db_config)
        engine = create_engine(url, client_encoding='utf8')
        meta = MetaData(bind=engine, reflect=True)
        print('Database connected successfully.')
        return engine, meta


    def get_table_obj(self, table_name: str, columns_info: {}):
        columns = get_column_list(columns_info)
        table = Table(table_name, self.meta, *columns)
        return table

    def create_tables(self, tables_info: {}):
        """
        tables_info is a dictionary of dictionary keyed by table names and columns information.
        """
        try:
            for table in tables_info:
                self.get_table_obj(table, tables_info[table])
            self.meta.create_all(self.engine)
            print(', '.join(list(tables_info.keys())) + ' tables have been successfully created.')
        except Exception as e:
            raise e


    def create_a_table(self, table_name: str, columns_info: {}):
        try:
            self.get_table_obj(table_name, columns_info)
            self.meta.create_all(self.engine)
            print('Table ' + table_name + ' has been successfully created.')
        except Exception as e:
            raise e


    def drop_a_table(self, table_name: str):
        try:
            table_obj = self.meta.tables[table_name]
            table_obj.drop(self.engine)
            print('Table ' + table_name + 'successfully deleted.')
        except Exception as e:
            raise e


    def drop_tables(self, table_names=None):
        """
        Warning: Default to drop all tables
        """
        try:
            if table_names is None:
                self.meta.drop_all(bind=self.engine)
                print('All tables have been deleted!!')
            else:
                table_obj_list = []
                table_names = as_list(table_names)
                for table in table_names:
                    table_obj_list.append(self.meta.tables[table])
                self.meta.drop_all(bind=self.engine, tables=table_obj_list)
                print(', '.join(table_names) + 'tables have been deleted!!')
        except Exception as e:
            raise e


    def delete_all_data_in_a_table(self, table_name: str):
        try:
            table = self.meta.tables[table_name]
            clause = table.delete()
            self.engine.execute(clause)
            print('All data in table ' + table_name + ' have been erased.')
        except Exception as e:
            raise e


    def select_table(self, table_name: str, where: {}=None):
        table_obj = self.meta.tables[table_name]
        clause = select([table_obj])
        if where is not None:
            or_condition = []
            for column_name in where:
                or_condition.append(table_obj.columns[column_name] == where[column_name])
            clause = clause.where(or_(*or_condition))
        df_columns = table_obj.columns.keys()
        data = list(self.engine.execute(clause))
        df = DataFrame(data, columns=df_columns)
        if len(df) == 0:
            raise EmptyDataError
        else:
            return df


    def upsert_clause(self, table_name: str, data: []):
        table_obj = self.meta.tables[table_name]
        clause = insert(table_obj).values(data)
        if len(table_obj.primary_key.columns) > 0:
            clause = clause.on_conflict_do_update(set_=dict(clause.excluded), constraint=table_obj.primary_key)
        return clause


    @staticmethod
    def as_batch_for_upsert(data: [], batch_size=1):
        length = len(data)
        return [data[i:min(i + batch_size, length)] for i in range(0, length, batch_size)]


    def upsert_data(self, table_name: str, data, batch_size=200):
        try:
            print('Upserting new data into table ' + table_name + '...')
            data = convert_data_format_for_upsert(data)
            data_batches = DatabaseQuery.as_batch_for_upsert(data, batch_size)
            clauses = [self.upsert_clause(table_name, each_batch) for each_batch in data_batches]
            for index, clause in enumerate(clauses):
                self.engine.execute(clause)
                print('Successfully upserted data batch number: ' + str(index + 1))
        except Exception as e:
            raise e

# =========================================== general implementation finishes ======================================

# ========================================= specific methods for some projects start =================================

    def select_time_series_table(self, table_name: str, **where_dates):
        '''
        Specifically designed for HL model where the primary key column header is 'Date'
        '''
        table = self.meta.tables[table_name]
        clause = select([table])

        if 'date' not in where_dates or where_dates['date'] is None:
            if 'from_dt' not in where_dates or where_dates['from_dt'] is None:
                pass
            else:
                clause = clause.where(where_dates['from_dt'] <= table.columns.Date)

            if 'to_dt' not in where_dates or where_dates['to_dt'] is None:
                pass
            else:
                clause = clause.where(where_dates['to_dt'] >= table.columns.Date)
        else:
            clause = clause.where(where_dates['date'] == table.columns.Date)

        cols = table.columns.keys()
        result = list(self.engine.execute(clause))
        result = DataFrame(result, columns=cols)
        if len(result) == 0:
            raise EmptyDataError
        else:
            return result
