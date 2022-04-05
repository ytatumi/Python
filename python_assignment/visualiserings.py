import sqlite3
import pandas as pd
from sqlalchemy import Integer
import datahanterings as dh
import matplotlib.pyplot as plt
import seaborn as sns


class visualiserings:
    def __init__(self, country, db, cur):
        self.country = country
        self.db = db
        self.cur = cur


    def _extract_country_data (self)->pd.DataFrame:
        """ This is a private method that extracts data in dataframe format for the specfied country from the database.
                 Returns:
                    pd.DataFrame: DataFrames for the data in the specified country from the database
        """

        data_daily_vaccinations=pd.read_sql(self._extract_data_query("daily_vaccinations"), self.db, params=(self.country,))
        data_total_vaccinations =pd.read_sql(self._extract_data_query("total_vaccinations"), self.db, params=(self.country,))
        data_people_fully_vaccinated =pd.read_sql(self._extract_data_query("people_fully_vaccinated"), self.db, params=(self.country,))
        data_people_vaccinated =pd.read_sql(self._extract_data_query("people_vaccinated"), self.db, params=(self.country,))
        
        return  data_daily_vaccinations, data_total_vaccinations, data_people_vaccinated, data_people_fully_vaccinated
    
    
    def _extract_data_query(self, table_name:str)->str:
        """ This is a private method that generate a query(query) for extracting data in dataframe format for the speficied country from the database.
                Args:
                    table_name(str): table name in the database from which information will be extracted in dataframe.
                Returns
                    str: query(query) for creating a table for one-hot encoding data in the database
        """
        
        query = '''SELECT country.country, {0}.* FROM {0}
        LEFT JOIN country ON {0}.iso_code = country.iso_code
        WHERE country.country =(?)'''.format(table_name)
        return query

    def subplots(self, ax_name:str, df_name:pd.DataFrame, column_name:str, ylabel_name:str,x_ticks_n:Integer):
        """ This is a private method that generate a query(query) for extracting data in dataframe format for the speficied country from the database.
                Args:
                    ax_name (str): ax name that is used for subplotting
                    df_name(DataFrame):Dataframe where the data to be plotted exists.
                    columnname(str):Column name in the dataframe of the data to be plotted exists
                    ylabel_name(str): name of ylabel 
                    x_ticks_n(integer): how ogten shows x ticks
                Returns:
                    str: query(query) for creating a table for one-hot encoding data in the database
        """
        ax_name.plot(df_name['date'],df_name[column_name], label=column_name)
        ax_name.tick_params(labelrotation=20)
        ax_name.set_xticks(ax_name.get_xticks()[::x_ticks_n])
        ax_name.ticklabel_format(axis="y", style="plain")
        ax_name.set_xlabel('date')
        ax_name.set_ylabel(ylabel_name)
        ax_name.legend(loc="upper left")
        ax_name.title.set_text(column_name)


    def plot_daily_vaccinations (self):
        """ This is a method that generate graphs based on the data extracted from the database 
        """
        
        data_daily_vaccinations, data_total_vaccinations, data_people_vaccinated, data_people_fully_vaccinated =self._extract_country_data()

        fig, ((ax1,ax2), (ax3, ax4), (ax5, ax6))= plt.subplots(3,2)
        self.subplots(ax1, data_daily_vaccinations, "daily_vaccinations","vaccinations",20)
        self.subplots(ax2, data_daily_vaccinations, "daily_vaccinations_per_million","vaccinations",20)
        self.subplots(ax3, data_total_vaccinations, "total_vaccinations","vaccinations",20)
        self.subplots(ax4, data_total_vaccinations, "total_vaccinations_per_hundred","vaccinations",20)
        self.subplots(ax5, data_people_vaccinated, "people_vaccinated","people",20)
        self.subplots(ax6, data_people_vaccinated, "people_vaccinated_per_hundred","people",20)
        self.subplots(ax5, data_people_fully_vaccinated, "people_fully_vaccinated","people",3)
        self.subplots(ax6, data_people_fully_vaccinated, "people_fully_vaccinated_per_hundred","people",3)
        fig.subplots_adjust(hspace=1.0)

        plt.style.use('ggplot')
        fig.suptitle(f"{self.country} Vaccination Trend", fontsize=14)
        plt.show()