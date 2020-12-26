# -*- coding: utf-8 -*-
#!/usr/bin/python
# March-May  2020  Gwyn Griffiths
# ts_batch_upload.py   a program to read in a spots file scraped from wsprnet.org by scraper.sh and upload to a TimescaleDB
# Version 1.2 May 2020 batch upload from a parsed file. Takes about 1.7s compared with 124s for line by line
# that has been pre-formatted with an awk line to be in the right order and have single quotes around the time and character fields
# Added additional diagnostics to identify which part of the upload fails (12 in 1936 times)
import psycopg2                  # This is the main connection tool, believed to be written in C
import psycopg2.extras           # This is needed for the batch upload functionality
import csv                       # To import the csv file
import sys                       # to get at command line argument with argv
import argparse

# initially set the connection flag to be None
conn=None
connected="Not connected"
cursor="No cursor"
execute="Not executed"
commit="Not committed"
ret_code=0

def ts_batch_upload(batch_file, sql, connect_info):
    global conn, connected, cursor, execute, commit, ret_code
    try:
        with batch_file as csv_file:
            csv_data = csv.reader(csv_file, delimiter=',')
            try:
                # connect to the PostgreSQL database
                #print ("Trying to  connect")
                conn = psycopg2.connect(connect_info)
                connected = "Connected"
                #print ("Appear to have connected")
                # create a new cursor
                cur = conn.cursor()
                cursor = "Got cursor"
                # execute the INSERT statement
                psycopg2.extras.execute_batch(cur, sql, csv_data)
                execute = "Executed"
                #print ("After the execute")
                # commit the changes to the database
                conn.commit()
                commit = "Committed"
                # close communication with the database
                cur.close()
                #print (connected,cursor, execute,commit)
            except:
                print("Unable to record spot file to the database:", connected, cursor, execute, commit)
                ret_code=1
    finally:
            if conn is not None:
                conn.close()
            sys.exit(ret_code)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Upload WSPRNET spots to Timescale DB')
    parser.add_argument("-i", "--input", dest="spotsFile", help="FILE is a CSV containing WSPRNET spots", metavar="FILE", required=True, nargs='?', type=argparse.FileType('r'), default=sys.stdin)
    parser.add_argument("-s", "--sql", dest="sqlFile", help="FILE is a SQL file containing an INSERT query", metavar="FILE", required=True, type=argparse.FileType('r'))
    parser.add_argument("-a", "--address", dest="address", help="ADDRESS is the hostname of the Timescale DB", metavar="ADDRESS", required=False, default="localhost")
    parser.add_argument("-d", "--database", dest="database", help="DATABASE is the database name in Timescale DB", metavar="DATABASE", required=True)
    parser.add_argument("-u", "--username", dest="username", help="USERNAME is the username to use with Timescale DB", metavar="USERNAME", required=True)
    parser.add_argument("-p", "--password", dest="password", help="PASSWORD is the password to use with Timescale DB", metavar="PASSWORD", required=True)
    args = parser.parse_args()

    with args.sqlFile as sql_file:
        sql = sql_file.read().strip()

    connect_info="dbname='%s' user='%s' host='%s' password='%s'" % (args.database, args.address, args.username, args.password)
    ts_batch_upload(batch_file=args.spotsFile, sql=sql, connect_info=None)