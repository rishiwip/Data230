import csv
import cx_Oracle
import io
import time
import json
dataList           = []
break_ct           = 100000
def oracleConnection():
    import csv
    import cx_Oracle
    import io
    import time
    try:
        conn = cx_Oracle.connect('*****/*****')
        #cur = conn.cursor()
        print("Connection established")
        return conn
    except Exception as e:
        print("Exception occurrred")

def processCSVData(fName):
    data = []
    fileLoc = "C:/SJSU/230-Data_Visualization/230project_ETL/"
    try:
        fName   = fName
        csvfile = open(fileLoc+fName,"rt")
        reader = csv.reader(csvfile, delimiter=',',lineterminator="\n")
        next(reader)        
        #for row in reader:
        #    data.append(row)
        return reader          
    except Exception as e:
        print("Exception Occurred:",str(e))

def getInsertSql(table):
    tb = table.split(".")
    sql = "SELECT COLUMN_ID, COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = '"+tb[1]+"' ORDER BY COLUMN_ID ASC"
    try:
        conn = oracleConnection()
        c = conn.cursor()
        res = c.execute(sql)
        col1 = []
        col2 = []
        for i in res:
            col1.append(str(i[0]))
            col2.append(i[1])
        str1 = ",:".join(col1)
        str2 = ",".join(col2)
        sql = "INSERT INTO "+ table +" ("+str2+ ") VALUES (:"+str1 +")"
        return sql
    except Exception as e:
        print("Exception occurred ")
        print(str(e))
        conn.rollback()
    finally:
        conn.commit()
        c.close()
        conn.close()
        print("Connection and Cursor Closed")


def loadData(fileName,table):
    import csv
    import io
    import time
    try:
        data = processCSVData(fileName)
        sql = getInsertSql(table)
        #print(sql)
        conn = oracleConnection()
        c = conn.cursor()
        ct = 0
        for i in data:
            ct+=1
            #print(i)
            try:
                c.execute(sql,i)
            except Exception as e:
                print("Exception occurred while executing sql:",str(e))
                print(i)
                
            if ct % 1000 == 0:
                print(str(ct),' : rows processed')
                conn.commit()
            #print(i)
        print("Table Loaded successfully")
    except Exception as e:
        print("Exception occurred ")
        print(str(e))
        raise
        conn.rollback()
    finally:
        conn.commit()
        c.close()
        conn.close()
        print("Connection and Cursor Closed")
        
def main():
    global dataList
    try:
        loadData("Source1_athlete_events.csv",'HR.OLYMPIC_RAW_HISTORY')
        loadData("Source2_API_GDP_PIVOT.csv",'HR.COUNTRIES_GDP')
        loadData("Source4_Country_API_NY.csv",'HR.GDP_COUNTRY_REGION_LKP')
        loadData("Source5_API_GDP_PER_CAPITA_PIVOT.csv",'HR.COUNTRIES_GDP')
    except Exception as e:
        print("Exception occurred",str(e))
    
if __name__ == '__main__':
    if main():
        True
    else:
        False
    