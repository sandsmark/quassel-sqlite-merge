#!/usr/bin/python

# GPLv2 or later

# Dependencies:
import psycopg2, sqlite3

# Set these as you see fit:
PG_CONN = "dbname=quassel user=quassel password='password' host=localhost"
SQLITE_DB = 'quassel-storage.sqlite'
IDENTITY = '1'
USER = '1'

## Nothing more to see beneath this

import sys, os
from pprint import pprint



pgcon = psycopg2.connect(PG_CONN)
c_target = pgcon.cursor()

sqliteconn = sqlite3.connect(SQLITE_DB);
c_source = sqliteconn.cursor()

def check_network_or_create(sourceid):
    c_source.execute("""
        SELECT
            networkname
        FROM network
        WHERE networkid = ?""", (sourceid,))
    source_info = c_source.fetchall()
    c_target.execute("SELECT networkid FROM network WHERE networkname = %s AND identityid = %s AND userid = %s", (source_info[0][0], IDENTITY, USER))
    results = c_target.fetchall()
    if (len(results) == 0):
        c_target.execute("INSERT INTO network(networkname, userid, identityid) VALUES(%s, %s, %s) RETURNING networkid", (source_info[0][0], USER, IDENTITY))
        results = c_target.fetchall()

    return results[0][0]

def check_buffer_or_create(bufferid):
    c_source.execute("SELECT groupid, networkid, buffername, buffercname, buffertype, key, joined FROM buffer WHERE bufferid = ?", (bufferid,))
    source_info = c_source.fetchall()
    network_id = source_info[0][1]
    network_id = check_network_or_create(network_id)
    buffer_name = source_info[0][2]
    buffer_cname = source_info[0][3]
    buffer_type = source_info[0][4]
    buffer_key = source_info[0][5]
    buffer_joined = source_info[0][6]
    if buffer_name == "":
        return -1

    c_target.execute("""SELECT bufferid FROM buffer WHERE buffername = %s""", (buffer_name,))
    results = c_target.fetchall()
    if buffer_joined == 0:
        buffer_joined = False
    else:
        buffer_joined = True

    if (len(results) == 0):
        c_target.execute("""
            INSERT INTO buffer(userid, networkid, buffername, buffercname,
            buffertype, key, joined) VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING bufferid""",
                (USER, network_id, buffer_name, buffer_cname, buffer_type, buffer_key, buffer_joined))
        results = c_target.fetchall()

    return results[0][0]


def check_sender_or_create(senderid):
    c_source.execute("SELECT sender FROM sender WHERE senderid = ?", (senderid,))
    source_sender = c_source.fetchall()[0][0]
    c_target.execute("SELECT senderid FROM sender WHERE sender = %s", (source_sender,))
    results = c_target.fetchall()
    if (len(results) == 0):
        c_target.execute("INSERT INTO sender(sender) VALUES(%s) RETURNING senderid", (source_sender,))
        results = c_target.fetchall()

    return results[0][0]

print("Merging...")
c_source.execute("SELECT time, bufferid, type, flags, senderid, message FROM backlog")
items = c_source.fetchall()
count = 0
lastpercent = 0
total = len(items)
for item in items:
    time = item[0]
    bufferid = check_buffer_or_create(item[1])
    if (bufferid == -1):
        continue
    btype = item[2]
    flags = item[3]
    senderid = check_sender_or_create(item[4])
    message = item[5]
    c_target.execute("""INSERT INTO backlog(time, bufferid, type, flags, senderid, message) VALUES((SELECT TIMESTAMP WITH TIME ZONE 'epoch' + %s * INTERVAL '1 second'), %s, %s, %s, %s, %s) RETURNING messageid""", (time, bufferid, btype, flags, senderid, message))

    count += 1
    percent = 100 * count / total
    if (percent != lastpercent):
        print("\033[K%d%%\033[1A" % percent)
        lastpercent = percent


pgcon.commit()
pgcon.close()
sqliteconn.close()
print("Done!")
