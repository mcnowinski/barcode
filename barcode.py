import readchar

import sys

import re

import sqlite3
from sqlite3 import Error

import datetime

import pandas as pd

db_path = r'./me2004rewards.db' # the database
csv_path = r'./me2004rewards.csv' # the csv output
# 2023/03/09 00:39:45 904643136
barcode_regex = r'^(\d{4})/(\d{2})/(\d{2}) (\d{2}):(\d{2}):(\d{2}) (\d+)$' # barcode scanner output (tab separated)

# create a connection to the database
def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        sys.exit(e)
    finally:
        return conn
    
def export_results_to_csv(conn):
    df = pd.read_sql_query("SELECT roster.student, roster.id, roster.sis_user_id, roster.section, roster.sis_login_id, COUNT(reward.sis_user_id) AS points FROM roster LEFT JOIN reward ON roster.sis_user_id = reward.sis_user_id GROUP BY roster.sis_user_id", conn)
    df.to_csv(csv_path, index=False, header=['Student', 'ID', 'SIS User ID', 'Section', 'SIS Login ID', 'Points'])
    print('Database written to %s.'%csv_path)

# main script
if __name__ == '__main__':
    # connect to ME 2004 rewards sqlite database
    conn = create_connection(db_path)
    cur = conn.cursor() # get ready to do queries

    while True:
        # process barcodes from scanner
        print('Waiting for barcode scanner input. Press <ESC> to abort.')
        barcode = ''
        while True:
            char = readchar.readchar()
            if char == '\t':  # tab delimited
                break
            elif char == chr(27):
                # update rewards
                export_results_to_csv(conn)                
                sys.exit('Aborted.')
            barcode = barcode + char # build final barcode
        # does scanner output match our desired format?
        match = re.search(barcode_regex, barcode)
        if match:
            sis_user_id = int(match.group(7)) # get student id
            # does this match a student we know?
            cur.execute('SELECT * FROM roster WHERE sis_user_id = %d;'%sis_user_id)
            row = cur.fetchone()
            if row != None: # yup! parse the rest!
                year = int(match.group(1))
                month = int(match.group(2))
                day = int(match.group(3))
                hr = int(match.group(4))
                min = int(match.group(5))
                sec = int(match.group(6))
                dt = datetime.datetime(year, month, day, hour=hr, minute=min, second=sec)
                # add this reward to the database
                try:
                    cur.execute("INSERT INTO reward ('dt','sis_user_id') VALUES (?,?)", (dt,sis_user_id)) # add a reward point
                    conn.commit()
                    print('Added reward for %s.'%row[0])
                except sqlite3.Error as e: # this can happen if a reward for that student and date/time has already been entered
                    print('Error. Failed to add reward for %s to database (%s).' % (row[0],(' '.join(e.args))))
            # update rewards
            export_results_to_csv(conn)                       
        else:   
            conn.close()
            sys.exit('Error. Invalid bar code scanner output (%s).'%barcode)
    conn.close()

# SELECT roster.student, roster.id, roster.sis_user_id, roster.section, roster.sis_login_id, COUNT(roster.sis_user_id) AS score FROM roster JOIN reward ON roster.sis_user_id = reward.sis_user_id GROUP BY roster.sis_user_id;            