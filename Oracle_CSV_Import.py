import csv
import cx_Oracle
import os
import time
os.environ["PATH"]='C:\\instantclient_11_2' #oracle instant client PATH variable
filepath=input("Enter File Path and Name:")#Import CSV file
Logfile=input("Enter Log File Name and Path:")#Log file update while inserting records
table=input("Database Table:") #Table name where need to dump data

def Log_Writer(Error_Msg,Log_file_Path,Actual_Error,Statement): #USed for Writing Log while CSV import
    if os.path.isfile(Log_file_Path):
        log = open(Log_file_Path, 'a')
        log.write(time.asctime() + ":" + Error_Msg + Actual_Error+":"+ Statement + "\n")
    else:
        log = open(Logfile, 'w')
        log.write(time.asctime() + ":" + Error_Msg + Actual_Error+":"+ Statement + "\n")
try:
    conn = cx_Oracle.connect('username/password@localhost/XE')
    mycursor=conn.cursor()
except Exception as e:
    e=str(e)
    Log_Writer("Error_Database_Connectivity",Logfile,e,"")
    exit(1)

with open(filepath,mode='r') as file:
    csv_reader=csv.reader(file)
    state='insert into'+' '+table+' '+'values('
    for field in csv_reader:
        num_of_field=len(field)#Fetching Header Field Count and It will skip first Row
        break
    for i in csv_reader:
        try:
            for k in range(0,num_of_field):
                state=state+"'"+i[k]+"',"
            state=(state[:-1]+")")
        except Exception as e:
            e=str(e)
            l=str(i)
            Log_Writer("Error:Header Not Matching With Records", Logfile, e,l)
            continue
        else:
            pass
        try:
            mycursor.execute(state)
            conn.commit()
            Log_Writer("Success :",Logfile,"",state)
        except Exception as e:
            e = str(e)
            Log_Writer("ERROR :",Logfile,e,state)
        else:
            pass
        state = 'insert into'+' '+table+' '+'values('

