import pandas as pd
import datahanterings as dh
import visualiserings as v

def select_country(data_source:pd.DataFrame)->str:
    """This is a method that gets input(=name of a country) from users and raises an erorr 
    when the input is not a country found in the database.
            Args:
                data_source(pd.DataFrame): A dataframe for one-ho
            Returns:
                str: input(name of a country)
    """     
    user_input=input("Please enter the name of a country for the graph or E to exit the program  > ")
    if user_input not in data_source['country'].values and user_input!="E":
        raise ValueError("can not find the country that you entered.")
    elif user_input=="E":
        print("Exiting")
    return user_input

def main():
    data_source= pd.read_csv(r'Assignment\\vaccin_covid.csv')  
    vac_data = dh.Datahanterings(r'Assignment\\vaccination.db', data_source)
    vac_data.seed_database()
    vac_data.normalisering()

    country=''
    while country=='':
        try:
            country=select_country(data_source)
            if country!="E":
                country_data=v.visualiserings(country,vac_data.db,vac_data.cur)
                country_data.plot_daily_vaccinations()
        except ValueError as e:
            print(e)
        
    vac_data.close_database()

if __name__ == "__main__":
    main()
