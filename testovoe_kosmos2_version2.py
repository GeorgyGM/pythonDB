import sqlite3
import json
import pandas as pd

class db_connector:
    @staticmethod
    def connect(db_path):
        '''Connect with db'''
        db_connector.conn = sqlite3.connect(db_path)
        db_connector.cursor = db_connector.conn.cursor()
        
    @staticmethod
    def connect_close():
        '''Close connect with db'''
        db_connector.cursor.close()
        db_connector.conn.commit()
        db_connector.conn.close()
        
    @staticmethod 
    def get_all_tables():
        '''Return list of tables (created for debug)'''
        db_connector.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        return db_connector.cursor.fetchall()
    
    @staticmethod 
    def create_table(table_name, parameters):
        '''Create new table'''
        # {field: [...]}
        fields_str = '('+', '.join(([field + ' ' + ' '.join(parameters[field]) for field in parameters.keys()]))+')'
        sql = f"CREATE TABLE {table_name} {fields_str}"
        return db_connector.cursor.execute(sql)
    
    @staticmethod
    def get_all(table_name):
        '''Return all rows from table'''
        db_connector.cursor.execute(f"SELECT * FROM {table_name}")
        return db_connector.cursor.fetchall()
    
    @staticmethod
    def insert_row(table_name, data):
        '''Insert new row into table'''
        # data = {field : value, ...}
        field_str, params_str = '(' + ', '.join(data.keys()) + ')', '(' + ', '.join(['?' for i in data]) + ')'
        sql = f"INSERT INTO {table_name} {field_str} VALUES {params_str}"
        return db_connector.cursor.execute(sql, list(data.values()))
        
    @staticmethod
    def insert_many_rows(table_name, data):
        '''Insert many rows into table'''
        # data = [{field : value}]
        field_str, params_str = '(' + ', '.join(data[0].keys()) + ')', '(' + ', '.join(['?' for i in data[0]]) + ')'
        sql = f"INSERT INTO {table_name} {field_str} VALUES {params_str}"
        return db_connector.cursor.executemany(sql, [list(d.values()) for d in data])
    
    @staticmethod
    def get(sql, params=False):
        '''Return rows by query'''
        if params:
            db_connector.cursor.execute(sql, params)
        else:
            db_connector.cursor.execute(sql)
        return db_connector.cursor.fetchall()
    
    @staticmethod
    def describe_table(table_name):
        '''Return table info'''
        return db_connector.get(f"pragma table_info('{table_name}');")
    
    @staticmethod
    def get_table_head(table_name):
        '''Return table head'''
        table_info = db_connector.describe_table(table_name)
        return [col[1] for col in table_info]
    
    @staticmethod
    def convert_table_to_df(table_name):
        '''Return table as DataFrame (created for debug)'''
        return pd.read_sql_query(f"SELECT * FROM {table_name}", db_connector.conn)
        
    
class SystemDB(db_connector):
    @staticmethod
    def generate_db():
        '''Create all tables for our system'''
        tables = {
            'types' : {
                'id': ['INTEGER', 'PRIMARY KEY', 'AUTOINCREMENT'], 
                'name': ['VARCHAR']
            },
            'users' : {
                'id': ['INTEGER', 'PRIMARY KEY', 'AUTOINCREMENT'],
                'login': ['VARCHAR', 'NOT NULL'], 
                'password': ['VARCHAR', 'NOT NULL'],
                'type': ['INTEGER', 'NOT NULL'],
                'inn': ['INTEGER', 'NOT NULL'], 
                'kpp': ['INTEGER'], 
                'ogrn': ['INTEGER'],
                'snils': ['INTEGER'],
                'org_full_name': ['TEXT'],
                'surname': ['VARCHAR'],
                'name': ['VARCHAR'],
                'middle_name': ['VARCHAR'],
                'address': ['TEXT', 'NOT NULL']
            }
        }
        types = [
            {'name': 'ИП'},
            {'name': 'Юридическое лицо'},
            {'name': 'Физическое лицо'}
        ]
        for table in tables.keys():
            super(SystemDB, SystemDB).create_table(table, tables[table])
        super(SystemDB, SystemDB).insert_many_rows('types', types)
        
    @staticmethod
    def add_user(user_data):
        '''Add new user'''
        # user_data = {field : value, ...}
        required_fields = ['login', "password", 'type', 'inn', 'address']
        if set(set(required_fields)&set(user_data.keys())) != set(required_fields):
            return False
        additionals_fields = ['ogrn', 'snils', 'surname', 'name', 'middle_name'] if user_data['type'] == 1 else ['kpp', 'ogrn', 'org_full_name'] if user_data['type'] == 2 else ['snils', 'surname', 'name', 'middle_name']
        all_fields = set(set(required_fields)|set(additionals_fields))
        if all_fields != set(user_data.keys()):
            return False
        return super(SystemDB, SystemDB).insert_row('users', user_data) #hash
    
    @staticmethod
    def get_fullname(login):
        '''Return user fullname'''
        user_type = super(SystemDB, SystemDB).get('SELECT type FROM users WHERE login = ?', [login])[0][0]
        if not user_type:
            return False
        sql = "SELECT org_full_name FROM users WHERE login = ?" if int(user_type) == 2 else "SELECT surname, name, middle_name FROM users WHERE login = ?"
        return ' '.join(super(SystemDB, SystemDB).get(sql, [login])[0])
    
    @staticmethod
    def get_user_attr(login):
        '''Return user attributes'''
        main_fields = set(['id', 'login', 'password', 'type'])
        fields = list(set(super(SystemDB, SystemDB).get_table_head('users')) - main_fields)
        fields_str = ', '.join(fields)
        sql = f"SELECT {fields_str} FROM users WHERE login = ?"
        res = super(SystemDB, SystemDB).get(sql, [login])[0]
        res_dict = dict(zip(fields, res))
        return json.dumps({key:res_dict[key] for key in res_dict.keys() if res_dict[key] is not None})
    
    @staticmethod
    def get_user_type(login):
        '''Return user type'''
        sql = "SELECT types.name FROM users, types WHERE users.type=types.id and login = ?"
        return super(SystemDB, SystemDB).get(sql, [login])[0][0]