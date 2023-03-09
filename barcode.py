import readchar

import sys

import re

import sqlite3
from sqlite3 import Error

import datetime

db_path = r'./me2004rewards.db' # the database
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

# main script
if __name__ == '__main__':
    # connect to ME 2004 rewards sqlite database
    conn = create_connection(db_path)
    cur = conn.cursor() # get ready to do queries

    while True:
        # process barcodes from scanner
        print('Waiting for barcode scanner input. Press <TAB> to abort.')
        barcode = ''
        while True:
            char = readchar.readchar()
            if char == '\t':  # tab delimited
                break
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
                    cur.execute("insert into reward ('dt','sis_user_id') values (?,?)", (dt,sis_user_id))
                    conn.commit()
                    print('Added reward for %s.'%row[0])
                except sqlite3.Error as e: # this can happen if a reward for that student and date/time has already been entered
                    print('Error. Failed to add reward for %s to database (%s).' % (row[0],(' '.join(e.args))))   
        else:
            sys.exit('Error. Invalid bar code scanner output (%s).'%barcode)