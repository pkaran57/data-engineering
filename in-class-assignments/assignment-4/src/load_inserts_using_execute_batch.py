# this program loads Census ACS data using basic, slow INSERTs 
# run it with -h to see the command line options

import argparse
import csv
import time

import psycopg2
import psycopg2.extras

DBname = "census_db"
DBuser = "pkaran"
DBpwd = "800"
TableName = 'CensusData'
Datafile = "filedoesnotexist"  # name of the data file to be loaded
CreateDB = False  # indicates whether the DB table should be (re)-created
Year = 2015


def row2vals(row):
    # handle the null vals
    for key in row:
        if not row[key]:
            row[key] = 0
        row['County'] = row['County'].replace('\'', '')  # eliminate quotes within literals

    row['Year'] = Year

    return row


def initialize():
    global Year

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--datafile", required=True)
    parser.add_argument("-c", "--createtable", action="store_true")
    parser.add_argument("-y", "--year", default=Year)
    args = parser.parse_args()

    global Datafile
    Datafile = args.datafile
    global CreateDB
    CreateDB = args.createtable
    Year = args.year


# read the input data file into a list of row strings
# skip the header row
def readdata(fname):
    print(f"readdata: reading from File: {fname}")
    with open(fname, mode="r") as fil:
        dr = csv.DictReader(fil)
        headerRow = next(dr)
        # print(f"Header: {headerRow}")

        rowlist = []
        for row in dr:
            rowlist.append(row)

    return rowlist


# convert list of data rows into list of SQL 'INSERT INTO ...' commands
def getSQLcmnds(rowlist):
    cmdlist = []
    for row in rowlist:
        valstr = row2vals(row)
        cmd = f"INSERT INTO {TableName} VALUES ({valstr});"
        cmdlist.append(cmd)
    return cmdlist


# connect to the database
def dbconnect():
    connection = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd,
    )
    connection.autocommit = True
    return connection


# create the target table
# assumes that conn is a valid, open connection to a Postgres database
def createTable(conn):
    with conn.cursor() as cursor:
        cursor.execute(f"""
        	DROP TABLE IF EXISTS {TableName};
        	CREATE TABLE {TableName} (
            	Year                INTEGER,
              CensusTract         NUMERIC,
            	State               TEXT,
            	County              TEXT,
            	TotalPop            INTEGER,
            	Men                 INTEGER,
            	Women               INTEGER,
            	Hispanic            DECIMAL,
            	White               DECIMAL,
            	Black               DECIMAL,
            	Native              DECIMAL,
            	Asian               DECIMAL,
            	Pacific             DECIMAL,
            	Citizen             DECIMAL,
            	Income              DECIMAL,
            	IncomeErr           DECIMAL,
            	IncomePerCap        DECIMAL,
            	IncomePerCapErr     DECIMAL,
            	Poverty             DECIMAL,
            	ChildPoverty        DECIMAL,
            	Professional        DECIMAL,
            	Service             DECIMAL,
            	Office              DECIMAL,
            	Construction        DECIMAL,
            	Production          DECIMAL,
            	Drive               DECIMAL,
            	Carpool             DECIMAL,
            	Transit             DECIMAL,
            	Walk                DECIMAL,
            	OtherTransp         DECIMAL,
            	WorkAtHome          DECIMAL,
            	MeanCommute         DECIMAL,
            	Employed            INTEGER,
            	PrivateWork         DECIMAL,
            	PublicWork          DECIMAL,
            	SelfEmployed        DECIMAL,
            	FamilyWork          DECIMAL,
            	Unemployment        DECIMAL
         	);	
         	ALTER TABLE {TableName} ADD PRIMARY KEY (Year, CensusTract);
         	CREATE INDEX idx_{TableName}_State ON {TableName}(State);
    	""")

        print(f"Created {TableName}")


def load(conn, rlis):
    all_rows = (row2vals(row) for row in rlis)

    with conn.cursor() as cursor:
        print(f"Loading {len(rlis)} rows")
        start = time.perf_counter()

        psycopg2.extras.execute_batch(cursor, f"""
            INSERT INTO {TableName} VALUES (
                %(Year)s,
                %(CensusTract)s,
                %(State)s,
                %(County)s,
                %(TotalPop)s,
                %(Men)s,
                %(Women)s,
                %(Hispanic)s,
                %(White)s,
                %(Black)s,
                %(Native)s,
                %(Asian)s,
                %(Pacific)s,
                %(Citizen)s,
                %(Income)s,
                %(IncomeErr)s,
                %(IncomePerCap)s,
                %(IncomePerCapErr)s,
                %(Poverty)s,
                %(ChildPoverty)s,
                %(Professional)s,
                %(Service)s,
                %(Office)s,
                %(Construction)s,
                %(Production)s,
                %(Drive)s,
                %(Carpool)s,
                %(Transit)s,
                %(Walk)s,
                %(OtherTransp)s,
                %(WorkAtHome)s,
                %(MeanCommute)s,
                %(Employed)s,
                %(PrivateWork)s,
                %(PublicWork)s,
                %(SelfEmployed)s,
                %(FamilyWork)s,
                %(Unemployment)s
            );
        """, all_rows, page_size=1000)

        elapsed = time.perf_counter() - start
        print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')


def main():
    initialize()
    conn = dbconnect()
    rlis = readdata(Datafile)

    if CreateDB:
        createTable(conn)

    load(conn, rlis)


if __name__ == "__main__":
    main()
