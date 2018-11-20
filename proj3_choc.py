import sqlite3
import csv
import json

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'

def create_database():
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    
    # Create tables, and I don't even need to drop shit
    statement = '''
        CREATE TABLE IF NOT EXISTS Bars(
            'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Company' TEXT,
            'SpecificBeanBarName' TEXT,
            'REF' TEXT,
            'ReviewDate' TEXT,
            'CocoaPercent' REAL,
            'CompanyLocationId' INTEGER,
            'Rating' REAL,
            'BeanType' TEXT,
            'BroadBeanOriginId' INTEGER
        )
    '''
    cur.execute(statement)
    
    statement = '''
        CREATE TABLE IF NOT EXISTS Countries (
            'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Alpha2' TEXT,
            'Alpha3' TEXT,
            'EnglishName' TEXT,
            'Region' TEXT,
            'Subregion' TEXT,
            'Population' INTEGER,
            'Area' REAL
        )
    '''
    cur.execute(statement)
    
    # Commit your changes, and close that shit
    conn.commit()
    conn.close()
    
def load_data():
    with open(COUNTRIESJSON, 'r') as f:
        content = f.read()
        countries = json.loads(content)
        
        for country in countries:
            inputs = (country['alpha2Code'], country['alpha3Code'], country['name'], country['region'], country['subregion'], country['population'], country['area'])
            statement = '''
                INSERT INTO Countries (Alpha2, Alpha3, EnglishName, Region, Subregion, Population, Area) VALUES (?, ?, ?, ?, ?, ?, ?)
            '''
            cur.execute(statement, inputs)
    
    with open(BARSCSV) as f:
        csv_reader = csv.reader(f)
        line = 0
        for row in csv_reader:
            if line == 0:
                pass
            elif line == 1:
                statement = '''
                    INSERT INTO Bars (Company, SpecificBeanBarName, REF, ReviewDate, CocoaPercent, CompanyLocationId, Rating, BeanType, BroadBeanOriginId) 
                '''
                
    
# Part 2: Implement logic to process user commands
def process_command(command):
    return []


def load_help_text():
    with open('help.txt') as f:
        return f.read()

# Part 3: Implement interactive prompt. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        response = input('Enter a command: ')

        if response == 'help':
            print(help_text)
            continue

# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    create_database()
    load_data()
    interactive_prompt()
