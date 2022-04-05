from distutils.log import error
import sqlite3
import pandas as pd
import numpy as np

class Datahanterings:
    def __init__(self, database, data_source):
        self.database=database
        self.data_source=data_source
        db, cur = self.create_database()
        self.cur = cur
        self.db =db
            
    def create_database(self):
        """This is a method that creates database and the connection to the database
        """ 
        try:
            db= sqlite3.connect(self.database) 
            cur = db.cursor()
            return db, cur
        except sqlite3.Error as e:
            print("An error while creating sqlite3.database", e)

    
    def close_database(self):        
        """This is a method that closes the connection to the database
        """ 
        self.cur.close()   
        self.db.close()
    
    def seed_database(self): 
        """ This is a method that seeds the database with data source(raw_data) .
        """
        self.cur.execute("DROP TABLE IF EXISTS raw_data;")
        self.data_source.to_sql('raw_data', self.db)

    def create_table(self,table_name:str, create_table_query:str): 
        """ This is a method that creates a table in database based on query(create_table_query)
                Args:
                    table_name:(str): table name that will be created by the query(create_table_query)
                    create_table_query (str): query for creating table
        """

        self.cur.execute ('''DROP TABLE IF EXISTS {};'''.format(table_name))
        self.cur.execute(create_table_query)
        self.cur.execute("PRAGMA foreign_keys = ON")
        self.db.commit()
    
    def insert_data_to_table(self, insert_data_query:str):
        """ This is a method that inserts data into a table in the database based on query(insert_data_query)
                Args:
                    insert_data_query (str): a query for inserting data into a table
        """

        self.cur.execute(insert_data_query)
        self.db.commit()
    
    def table_hantering(self, table_name:str, create_table_query:str, insert_data_query:str):
        """ This is a method that creates a table and inserts the data into the table in the database based on queries(create_table_query/insert_data_query)
                Args:
                    table_name (str): table name that will be created by the query(create_table_query)
                    create_table_query (str): query for creating table
                    insert_data_query (str): query for insert data into a table
        """
        self.create_table(table_name, create_table_query)
        self.insert_data_to_table(insert_data_query)
    
    def _vaccination_table_hantering(self, table_name:str, column_name1:str, column_name2:str):
        """ This is a private method that creates a table for vaccination data and insert data into the table in the database.
                Args:
                    table_name (str): table name that is created in the database.
                    column_name1:(str): column name in the table that is created in the database.
                    column_name2:(str): column name in the table that is created in the database.
        """
        create_table_query = self._create_table_query(table_name, column_name1, column_name2)
        insert_data_query = self._insert_data_query(table_name, column_name1, column_name2)
        self.create_table(table_name, create_table_query)
        self.insert_data_to_table(insert_data_query)

        
    def _create_table_query(self, table_name:str, column_name1:str, column_name2:str)->str: 
        """ This is a private method that generate a query(query) for creating a table for vaccination data in the database.
                Args:
                    table_name (str): table name that is created in the database.
                    column_name1:(str): column name in the table that is created in the database.
                    column_name2:(str): column name in the table that is created in the database.
                    str: query(query) for creating a table in database.
        """
        query='''CREATE TABLE {}(
            iso_code VARCHAR(10),
            date DATE, 
            {} INTEGER, 
            {} INTEGER,
            PRIMARY KEY(iso_code, date)
            FOREIGN KEY (iso_code)  REFERENCES country(iso_code)
            ) ''' .format(table_name, column_name1, column_name2)
        return query
    
    def _insert_data_query(self, table_name:str, column_name1:str, column_name2:str)-> str:
        """ This is a private method that generates a query(query) for inserting the vaccination data in a table in database.
                Args:
                    table_name (str): table name in the databse that the data is inserted in.
                    column_name1:(str): column name in the table in the database that the data is inserted in.
                    column_name2:(str): column name in the table in the database that is inserted in.
                    str: query(query) for inserting the data in a table in database.
        """
        table_info = {'t':table_name, 'c1':column_name1, 'c2':column_name2}
        query = ''' INSERT or REPLACE INTO {t} (iso_code, date, {c1}, {c2}) 
        SELECT iso_code, DATE(date), {c1}, {c2} FROM raw_data 
        WHERE {c1} IS NOT NULL or  {c2} IS NOT NULL'''.format(**table_info)
        return query 


        
    def one_hot_encoding_df(self, column_name:str, primary_key:str)-> pd.DataFrame: 
        """ This is a method that creates a table for one-hot encoding data((vaccines) in database.
                Args:
                    column_name(str): column name where one-hot encoding is applied
                    primary_key(str): one-hot encoding table's primary key
                Returns:
                    pd.DataFrame: A dataframe 
        """
        explode=self.data_source.assign(vaccines=self.data_source.vaccines.str.split(", ")).explode(column_name)

        ohe= pd.get_dummies(explode, columns=[column_name],prefix='', prefix_sep='', drop_first=False )
        ohe.columns =ohe.columns.str.replace("&","_").str.replace("/","_").str.replace("-","_").str.replace(" ","_")

        ohe_filtered=ohe.drop_duplicates([primary_key])
        ohe_filtered

        col = np.r_[0,2:14]
        ohe_filtered = ohe_filtered.drop(ohe_filtered.columns[col], axis=1)

        return ohe_filtered

    def one_hot_encoding (self, ohe_df, ohe_table_name:str, column_name:str, primary_key:str):
        """ This is a method that creates a table for one-hot encoding data in database.
                Args:
                    ohe_df(str): dataframe for one-hot encoding data
                    ohe_table_name(str):table name for one-hot encoding data
                    column_name(str): column name where one-hot encoding is applied
                    primary_key(str): one-hot encoding table's primary key
        """
        ohe_df =self.one_hot_encoding_df(column_name, primary_key) 
        create_ohe_q= self.create_ohe_query(ohe_df, ohe_table_name)
        self.create_table(ohe_table_name, create_ohe_q)
        ohe_df.to_sql(name=ohe_table_name, con=self.db, if_exists="append", index=False)

    
    def create_ohe_query(self,df_name:str,table_name:str)-> str:
        """ This is a method that generate a query(query) for creating a table for one-hot encoding data in database.
                Args:
                    df_name (str): dataframe name for one hot encoding data.
                    table_name:(str): table name for one-hot encoding that will be created by the query(query) that is generated by this method.
                Returns:
                    str: query(query) for creating a table for one-hot encoding data in the database
        """
        column_names=list(df_name)
        query = " CREATE TABLE " +table_name + "("
        for col in column_names:  
            if col == "iso_code":
                query += col + " VARCHAR(10),"
            else:
                query += col + " BIT,"       
        query += ''' PRIMARY KEY(iso_code)
            FOREIGN KEY (iso_code)  REFERENCES country(iso_code)
            );''' 
        return query
    
    
    def normalisering(self):
        """ This is a method that clean up/select the data (=delete NaN and exclude uninteresting column) and creates tables for relational database.
        """
        
        create_country_q=''' CREATE TABLE country(
            iso_code varchar(10) PRIMARY KEY,
            country varchar(100)
            ) '''       
    
        insert_country_q =''' INSERT or REPLACE INTO country(iso_code, country) 
        SELECT DISTINCT(iso_code), country FROM raw_data'''
        
        self.table_hantering("country",create_country_q,insert_country_q)
       
        self._vaccination_table_hantering("daily_vaccinations", "daily_vaccinations", "daily_vaccinations_per_million")
        self._vaccination_table_hantering("total_vaccinations", "total_vaccinations", "total_vaccinations_per_hundred")
        self._vaccination_table_hantering("people_vaccinated", "people_vaccinated", "people_vaccinated_per_hundred")
        self._vaccination_table_hantering("people_fully_vaccinated", "people_fully_vaccinated", "people_fully_vaccinated_per_hundred")

        self.one_hot_encoding("vaccine_ohe_df", "vaccine_ohe", "vaccines","iso_code")




