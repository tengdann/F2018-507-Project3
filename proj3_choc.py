import sqlite3
import csv
import json
import sys
import codecs
import fnmatch
sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer)

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'

try:
    INPUT = sys.argv[1]
except:
    INPUT = None

def create_database():
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    
    # Drop tables in case of bad data
    statement = '''
        DROP TABLE IF EXISTS Bars
    '''
    cur.execute(statement)
    
    statement = '''
        DROP TABLE IF EXISTS Countries
    '''
    cur.execute(statement)
    
    conn.commit()
    print('Old tables successfully deleted')
    
    # Create tables
    statement = '''
        CREATE TABLE Bars(
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
        CREATE TABLE Countries (
            'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Alpha2' TEXT,
            'Alpha3' TEXT,
            'EnglishName' TEXT UNIQUE,
            'Region' TEXT,
            'Subregion' TEXT,
            'Population' INTEGER,
            'Area' REAL
        )
    '''
    cur.execute(statement)
    
    # Commit changes and close
    conn.commit()
    print('New tables successfully created')
    conn.close()
    
def load_data():
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    
    with open(COUNTRIESJSON, encoding = 'utf-8') as f:
        content = f.read()
        countries = json.loads(content)
        
        statement = '''
            INSERT OR IGNORE INTO Countries (Alpha2, Alpha3, EnglishName, Region, Subregion, Population, Area) VALUES (?, ?, ?, ?, ?, ?, ?)
        '''
        cur.execute(statement, ('UN', 'UNK', 'Unknown', 'Unknown', 'Unknown', 0, 0))
        
        for country in countries:
            inputs = (country['alpha2Code'], country['alpha3Code'], country['name'], country['region'], country['subregion'], country['population'], country['area'])
            cur.execute(statement, inputs)
            
        f.close()
            
    conn.commit()
    
    with open(BARSCSV, encoding = 'utf-8') as f:
        csv_reader = csv.reader(f)
        line = 0
        for row in csv_reader:
            if line == 0:
                pass
                line += 1
            else:
                for item in row:
                    if item == '':
                        item = 'NULL'
                        
                inputs = (row[5],)
                statement = '''
                    SELECT Id FROM Countries WHERE EnglishName = ?
                '''
                cur.execute(statement, inputs)
                try:
                    location_id = cur.fetchone()[0]
                except:
                    location_id = 'NULL'
                
                inputs = (row[8],)
                statement = '''
                    SELECT Id FROM Countries WHERE EnglishName = ?
                '''
                cur.execute(statement, inputs)
                try:
                    origin_id = cur.fetchone()[0]
                except:
                    origin_id = 'NULL'
                
                inputs = (row[0], row[1], row[2], row[3], row[4].split('%')[0], location_id, row[6], row[7], origin_id,   row[0], row[1], row[2], row[3], row[4].split('%')[0], location_id, row[6], row[7], origin_id)
                statement = '''
                    INSERT INTO Bars (Company, SpecificBeanBarName, REF, ReviewDate, CocoaPercent, CompanyLocationId, Rating, BeanType, BroadBeanOriginId)

                    SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?
                    WHERE NOT EXISTS(SELECT 1 FROM Bars WHERE Company = ? AND SpecificBeanBarName = ? AND REF = ? AND ReviewDate = ? AND CocoaPercent = ? AND CompanyLocationId = ? AND Rating = ? AND BeanType = ? AND BroadBeanOriginId = ?)
                '''
                cur.execute(statement, inputs)
                line += 1
                
        f.close()
                
    conn.commit()
    conn.close()
                
    
# Part 2: Implement logic to process user commands
def process_command(command):
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    
    command_split = command.split(' ')
    
    if command_split[0].lower() == 'bars':        
        company_country = fnmatch.filter(command_split, 'sellcountry*')
        if len(company_country) != 0:
            company_country = company_country[0].split('=')[1]
        else:
            company_country = '%'
        
        bean_country = fnmatch.filter(command_split, 'sourcecountry*')
        if len(bean_country) != 0:
            bean_country = bean_country[0].split('=')[1]
        else:
            bean_country = '%'
        
        company_region = fnmatch.filter(command_split, 'sellregion*')
        if len(company_region) != 0:
            company_region = company_region[0].split('=')[1]
        else:
            company_region = '%'
        
        bean_region = fnmatch.filter(command_split, 'sourceregion*')
        if len(bean_region) != 0:
            bean_region = bean_region[0].split('=')[1]
        else:
            bean_region = '%'
            
        if 'cocoa' in command_split:
            column = 'CocoaPercent'
        else:
            column = 'Rating'
        
        delimit_top = fnmatch.filter(command_split, 'top*')
        order = 'desc'
        limit = 10
        if len(delimit_top) != 0:
            limit = int(delimit_top[0].split('=')[1])
            
        delimit_bottom = fnmatch.filter(command_split, 'bottom*')
        if len(delimit_bottom) != 0:
            order = 'asc'
            limit = int(delimit_bottom[0].split('=')[1])
                  
        statement = '''
            SELECT SpecificBeanBarName, Company, c.EnglishName, Rating, CocoaPercent, c1.EnglishName
            FROM Bars AS b
                JOIN Countries AS c
                ON b.CompanyLocationId = c.Id
                JOIN Countries AS c1
                ON b.BroadBeanOriginId = c1.Id
            WHERE c.Alpha2 LIKE ? AND c1.Alpha2 LIKE ? AND c.Region LIKE ? AND c1.Region LIKE ?
            ORDER BY {} {}
            LIMIT ?
        '''.format(column, order)
        
        cur.execute(statement,(company_country, bean_country, company_region, bean_region, limit))
        return cur.fetchall()
        
    elif command_split[0].lower() == 'companies':
<<<<<<< HEAD
        country = fnmatch.filter(command_split, 'country*')
        if len(country) != 0:
            country = country[0].split('=')[1]
        else:
            country = '%'
            
        region = fnmatch.filter(command_split, 'region*')
        if len(region) != 0:
            region = region[0].split('=')[1]
        else:
            region = '%'
            
        delimit_top = fnmatch.filter(command_split, 'top*')
        order = 'desc'
        limit = 10
        if len(delimit_top) != 0:
            limit = int(delimit_top[0].split('=')[1])
            
        delimit_bottom = fnmatch.filter(command_split, 'bottom*')
        if len(delimit_bottom) != 0:
            order = 'asc'
            limit = int(delimit_bottom[0].split('=')[1])
            
        if 'cocoa' in command_split:
            agg = 'AVG(CocoaPercent)'
        elif 'bars_sold' in command_split:
            agg = 'Count(*)'
        else:
            agg = 'AVG(Rating)'
        
        statement = '''
            SELECT Company, c.EnglishName, {}
            FROM Bars AS b
                JOIN Countries AS c
                ON b.CompanyLocationId = c.Id
            WHERE c.Alpha2 LIKE ? AND c.Region LIKE ?
            GROUP BY Company
            HAVING Count(*) > 4
            ORDER BY {} {}
            LIMIT ?
        '''.format(agg, agg, order)
        
        cur.execute(statement, (country, region, limit))
        return cur.fetchall()
        
=======
        statement = '''
            SELECT Company, c.EnglishName, Count(*)
            FROM Bars AS b
                JOIN Countries AS c
                ON b.CompanyLocationId = c.Id
            WHERE c.Alpha2 LIKE '%' AND c.Region LIKE '%'
            GROUP BY Company
            HAVING Count(*) > 4
            ORDER BY Count(*) desc
            LIMIT 10
        '''
>>>>>>> 316829d3f2f2aa26ce3a63aee14d5c6b2e0cc08f
    elif command_split[0].lower() == 'countries':
        region = fnmatch.filter(command_split, 'region*')
        if len(region) != 0:
            region = region[0].split('=')[1]
        else:
            region = '%'
            
        if 'sources' in command_split:
            group = 'BroadBeanOriginId'
        else:
            group = 'CompanyLocationId'
            
        delimit_top = fnmatch.filter(command_split, 'top*')
        order = 'desc'
        limit = 10
        if len(delimit_top) != 0:
            limit = int(delimit_top[0].split('=')[1])
            
        delimit_bottom = fnmatch.filter(command_split, 'bottom*')
        if len(delimit_bottom) != 0:
            order = 'asc'
            limit = int(delimit_bottom[0].split('=')[1])
            
        if 'cocoa' in command_split:
            agg = 'AVG(CocoaPercent)'
        elif 'bars_sold' in command_split:
            agg = 'Count(*)'
        else:
            agg = 'AVG(Rating)'
        
        statement = '''
            SELECT c.EnglishName, c.Region, {}
            FROM Bars AS b
                JOIN Countries AS c
                ON b.{} = c.Id
            WHERE c.Region LIKE ?
            GROUP BY {}
            HAVING Count(*) > 4
            ORDER BY {} {}
            Limit ?
        '''.format(agg, group, group, agg, order)
        
        cur.execute(statement, (region, limit))
        return cur.fetchall()
        
    elif command_split[0].lower() == 'regions':
        if 'sources' in command_split:
            group = 'BroadBeanOriginId'
        else:
            group = 'CompanyLocationId'
            
        delimit_top = fnmatch.filter(command_split, 'top*')
        order = 'desc'
        limit = 10
        if len(delimit_top) != 0:
            limit = int(delimit_top[0].split('=')[1])
            
        delimit_bottom = fnmatch.filter(command_split, 'bottom*')
        if len(delimit_bottom) != 0:
            order = 'asc'
            limit = int(delimit_bottom[0].split('=')[1])
            
        if 'cocoa' in command_split:
            agg = 'AVG(CocoaPercent)'
        elif 'bars_sold' in command_split:
            agg = 'Count(*)'
        else:
            agg = 'AVG(Rating)'
        
        statement = '''
            SELECT c.Region, {}
            FROM Bars AS b
                JOIN Countries AS c
                ON b.{} = c.Id
            WHERE c.Region != 'Unknown'
            GROUP BY c.Region
            HAVING Count(*) > 4
            ORDER BY {} {}
            Limit ?
        '''.format(agg, group, agg, order)
        
        cur.execute(statement, (limit,))
        return cur.fetchall()
        
    else:
        print('Invalid command!')
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
        else:
            process_command(response)

# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    if INPUT == 'init':
        create_database()
        print('Dropped and created new database tables')
    elif INPUT == 'load':
        load_data()
        print('Loaded json and csv files')
    else:
        interactive_prompt()
