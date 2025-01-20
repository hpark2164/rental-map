import csv
from dataclasses import dataclass
from mysql_utils import create_server_connection

@dataclass
class ZillowDataRow:
    id: int
    rank: int
    region_name: str
    region_type: str
    state_name: str
    state: str
    city: str
    metro: str
    county: str
    date: str
    rent_avg: float

    
def insert_rent_query(row):
    query = "INSERT INTO zillow_rent_data\
        (RegionID, SizeRank, RegionName, RegionType,\
            StateName, State, City, Metro, CountyName,\
                DateRecorded, AverageRent)\
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
    
    args = (row.id, row.rank, row.region_name, row.region_type, row.state_name, row.state, row.city, row.metro, row.county, row.date, row.rent_avg)
    
    print(args)
    return query, args

'''
Read zillow csv data into database for filtering operations
'''

# name function read_from_csv, with argument db_connection - this will connect us to the
# database we created on this mac on mySQL workbench.
def read_from_csv(db_connection): 
    # create variable that acts as cursor, so we can tell it where to start looking in database
    cursor = db_connection.cursor(prepared=True)
    # open the zillow csv and call it "rental_data" 
    with open ('zori_data_by_zip.csv') as rental_data:
        # create a reader to read in data from "rental_data" zillow csv
        # tell the reader that a comma is what separates the entries
        zori_reader = csv.reader(rental_data, delimiter=',')
        # create new list of strings called "header_row" which includes ALL column names that
        # are in the zillow csv's first row
        header_row = next(zori_reader)
        # create new list of strings called "date_cols" with ONLY date column names
        date_cols = header_row[9:]

        # for each row in the csv (after the headers since we already read that), 
        # get the rent data for each date and then put into "rent_info" which is an instance
        # of the class ZillowDataRow. After, insert rent_info into the SQL table.
        for row in zori_reader:
            for i in range(0, len(date_cols), 1):
                rent = row[i+9]
                if rent == "":
                    rent = None
                
                rent_info = ZillowDataRow(
                    id=row[0], rank=row[1], region_name=row[2], region_type=row[3],
                    state_name=row[4], state=row[5], city=row[6], metro=row[7], county=row[8],
                    date=date_cols[i], rent_avg=rent
                )
                query, args = insert_rent_query(rent_info)
                cursor.execute(query, args)
                db_connection.commit()

        print("done")
        

if __name__ == "__main__":
    db_connection = create_server_connection()
    read_from_csv(db_connection)
