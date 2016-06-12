#!/usr/bin/python2.7
#
# CSE 512 
# Author: Aakanxu Shah
#

import psycopg2

DATABASE_NAME = 'dds_assgn1'

con = None


def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):

    cur = openconnection.cursor()
    cur.execute("DROP TABLE IF EXISTS "+ratingstablename)
    cur.execute("CREATE TABLE "+ratingstablename+" (row_id serial primary key,UserID INT, temp1 VARCHAR(10),  MovieID INT , temp3 VARCHAR(10),  Rating REAL, temp5 VARCHAR(10), Timestamp INT)")
    
    loadout = open(ratingsfilepath,'r')
    
    cur.copy_from(loadout,ratingstablename,sep = ':',columns=('UserID','temp1','MovieID','temp3','Rating','temp5','Timestamp'))
    cur.execute("ALTER TABLE "+ratingstablename+" DROP COLUMN temp1, DROP COLUMN temp3,DROP COLUMN temp5, DROP COLUMN Timestamp")
    
    cur.close()
    


def rangepartition(ratingstablename, numberofpartitions, openconnection):

    cur = openconnection.cursor()
    global RangePart
    RangePart = numberofpartitions
    
    Range = 5.0/numberofpartitions
    i=1
    Demo = 0
    
    while Demo<5.0:
        if Demo == 0:
            cur.execute("DROP TABLE IF EXISTS range_part"+str(i))
            cur.execute("CREATE TABLE range_part"+str(i)+ " AS SELECT * FROM "+ratingstablename+" WHERE Rating>="+str(Demo)+ " AND Rating<="+str(Demo+Range)+";")
            i=i+1
            Demo = Demo + Range
        else:
            cur.execute("DROP TABLE IF EXISTS range_part"+str(i))
            cur.execute("CREATE TABLE range_part"+str(i)+" AS SELECT * FROM "+ratingstablename+" WHERE Rating>"+str(Demo)+ " AND Rating<="+str(Demo+Range)+";")
            i=i+1 
            Demo = Demo + Range
            
    cur.close()
                        


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    
    list_partition = list(reversed(range(numberofpartitions)))
    global NumPart
    NumPart = numberofpartitions
    global end
    end = 0
    k = 0
    
    for j in list_partition:
        cur.execute("DROP TABLE IF EXISTS rrobin_part"+str(j+1))
        cur.execute("CREATE TABLE rrobin_part"+str(j+1)+ " AS SELECT * FROM "+ratingstablename+" WHERE row_id % " + str(numberofpartitions) + " = " + str((j+1)%numberofpartitions))
        rowNo_partition = cur.execute("SELECT COUNT (*) FROM rrobin_part"+str(j+1)+";")
        
        if rowNo_partition>k:
            end = j
            k=rowNo_partition
        
    cur.close()
                        


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):

    cur = openconnection.cursor()

    global end
    global NumPart
    
    end_part = end % NumPart
    

    cur.execute("INSERT INTO rrobin_part"+str(end_part+1)+" (UserID,MovieID,Rating) VALUES ("+str(userid)+","+str(itemid)+","+str(rating)+")")

    
    cur.close()
    
def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()

    global RangePart

    range2 = 5.0 / RangePart
    

    Lower = 0
    partitionnumber = 0
    Upper = range2
    
    while Lower<5.0:
        if Lower == 0:
            if rating >= Lower and rating <= Upper:
                break
            partitionnumber = partitionnumber + 1
            Lower = Lower + range2
            Upper = Upper + range2
        else: 
            if rating > Lower and rating <= Upper:
                break
            partitionnumber = partitionnumber + 1
            Lower = Lower + range2
            Upper = Upper + range2
            
            
         
    cur.execute("INSERT INTO range_part"+str(partitionnumber)+" (UserID,MovieID,Rating) VALUES (%s, %s, %s)",(userid, itemid, rating))

    cur.close()
    
def deletepartitionsandexit(openconnection):

    cur = openconnection.cursor()
    
    cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE '{0}%';".format("range_part"))

    number = int(cur.fetchone()[0])

    if number != 0:
        
        for k in range(number):
           
           cur.execute("DROP TABLE IF EXISTS " + "range_part" + str(k+1) + ";")
            
	    
    cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE '{0}%';".format("rrobin_part"))

    number = int(cur.fetchone()[0])

    if number != 0:
        for l in range(number):
            cur.execute("DROP TABLE IF EXISTS " + "rrobin_part" + str(l+1) + ";")

    cur.close()

def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()


# Middleware
def before_db_creation_middleware():
    # Use it if you want to
    pass


def after_db_creation_middleware(databasename):
    # Use it if you want to
    pass


def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    pass


if __name__ == '__main__':

    
        
    try:

        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()

        create_db(DATABASE_NAME)

        # Use this function to do any set up after creating the DB, if any
        after_db_creation_middleware(DATABASE_NAME)

        with getopenconnection() as con:
            # Use this function to do any set up before I starting calling your functions to test, if you want to
            before_test_script_starts_middleware(con, DATABASE_NAME)

            # Here is where I will start calling your functions to test them. For example,
                  
            loadratings('Ratings', 'ratings.dat', con)
            #rangepartition(ratingstablename = 'Ratings', numberofpartitions = 5, openconnection = con)
            #roundrobinpartition(ratingstablename = 'Ratings', numberofpartitions = 5, openconnection = con)
            #roundrobininsert(ratingstablename = 'Ratings', userid = 1, itemid = 2, rating = 5, openconnection = con)
            #rangeinsert(ratingstablename = 'Ratings', userid = 2, itemid = 2, rating = 4, openconnection = con)
            #rangeinsert(ratingstablename = 'Ratings', userid = 3, itemid = 3, rating = 3, openconnection = con)
            

                
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
        
# Disclaimer for students : Be Original and Do not plagiarize ! :)
