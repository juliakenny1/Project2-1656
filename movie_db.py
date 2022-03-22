import sqlite3 as lite
import csv
import re
import pandas as pd
import argparse
import collections
import json
import glob
import math
import os
import requests
import string
import sqlite3
import sys
import time
import xml


class Movie_db(object):
    def __init__(self, db_name):
        #db_name: "cs1656-public.db"
        self.con = lite.connect(db_name)
        self.cur = self.con.cursor()

    #q0 is an example
    def q0(self):
        query = '''SELECT COUNT(*) FROM Actors'''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q1(self):
        query = '''
        SELECT Actors.fname, Actors.lname
        FROM Cast AS c INNER JOIN Actors ON c.aid = Actors.aid
        WHERE c.aid in (SELECT c.aid
                        FROM Cast AS c INNER JOIN Movies AS m ON m.mid = c.mid
                        WHERE m.year < 1990 AND m.year > 1980)
        AND c.aid in (SELECT c.aid
                        FROM Cast AS c INNER JOIN Movies AS m ON m.mid = c.mid
                        WHERE m.year >= 2000)
        GROUP BY c.aid

        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows


    def q2(self):
        query = '''
        SELECT title, year
        FROM Movies
        WHERE year in (SELECT Movies.year FROM Movies WHERE title = "Rogue One: A Star Wars Story")
        AND rank > (SELECT rank FROM Movies WHERE title = "Rogue One: A Star Wars Story")
        ORDER BY title ASC;
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q3(self):
        self.cur.execute('DROP VIEW IF EXISTS v')
        query = '''
        create view v as SELECT DISTINCT c.aid, count(*) as num
		FROM Movies m, Cast c WHERE m.mid = c.mid AND m.title LIKE '%Star Wars%'
		GROUP BY c.aid HAVING num >= 1
        '''
        self.cur.execute(query)

        query = '''
        SELECT a.fname, a.lname, v.num
    	FROM Actors a,  v
    	WHERE a.aid = v.aid
    	ORDER BY v.num DESC, a.lname ASC, a.fname ASC
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows


    def q4(self):
        query = '''
        SELECT a.fname, a.lname
        FROM Actors as a INNER JOIN Cast as c on a.aid=c.aid
        INNER JOIN Movies as m ON c.mid=m.mid
        WHERE NOT a.aid IN (SELECT actor.aid
                        FROM Actors AS actor
                        INNER JOIN Cast AS casts ON actor.aid = casts.aid
                        INNER JOIN Movies AS movie ON movie.mid = casts.mid
                        WHERE movie.year > 1980)
        GROUP BY a.fname , a.lname
        ORDER BY a.lname ASC, a.fname ASC
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q5(self):
        query = '''
        SELECT Directors.fname, Directors.lname, count(*) as c
        FROM Directors
        INNER JOIN Movie_Director m on m.did = Directors.did
        GROUP BY Directors.lname, Directors.fname
        ORDER BY c DESC , Directors.lname, Directors.fname
        LIMIT 10
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q6(self):
        query = '''
        SELECT m.title, COUNT(DISTINCT c.aid) AS cast_count
        FROM Movies as m INNER JOIN Cast as c ON c.mid = m.mid
        GROUP BY m.mid
        HAVING cast_count >= (SELECT MIN(n)
                            FROM (SELECT COUNT(ca.aid) AS n
                                  FROM Movies as mo INNER JOIN Cast as ca ON ca.mid = mo.mid
                                  GROUP BY mo.mid
                                  ORDER BY n DESC
                                  LIMIT 10))
        ORDER BY cast_count DESC
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q7(self):
        self.cur.execute('DROP VIEW IF EXISTS quesA')
        self.cur.execute(';')
        query = '''
        CREATE VIEW quesA AS SELECT m.title as title, a.gender AS gender
        FROM Actors as a INNER JOIN Cast as c on c.aid = a.aid
        INNER JOIN Movies as m on c.mid = m.mid
        GROUP BY fname, lname, m.title
        '''
        self.cur.execute(query)
        self.cur.execute('DROP VIEW IF EXISTS quesB')
        query = '''
        CREATE VIEW quesB AS SELECT quesA.title as title,
        sum(CASE WHEN quesA.gender = "Female" THEN 1 ELSE 0 END)
        AS FEM, sum(CASE WHEN quesA.gender = "Male" THEN 1 ELSE 0 END)
        AS MALE FROM quesA GROUP BY quesA.title
        '''
        self.cur.execute(query)
        query = '''
        SELECT quesB.title, quesB.FEM, quesB.MALE
        FROM quesB
        WHERE quesB.FEM > quesB.MALE
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q8(self):
        query = '''
        SELECT a.fname, a.lname, COUNT(DISTINCT movDir.did) AS ct
        FROM Actors AS a
        INNER JOIN Cast AS c ON a.aid = c.aid
        INNER JOIN Movie_Director AS movDir ON c.mid = movDir.mid
        GROUP BY a.aid, a.fname, a.lname
        HAVING ct >=7
        ORDER BY ct DESC, a.lname,a.fname
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows


    def q9(self):
        query = '''
        DROP VIEW IF EXISTS startD
        '''
        self.cur.execute(query)

        query = '''
        CREATE VIEW startD AS SELECT Actors.aid, Actors.fname, Actors.lname
        FROM Actors WHERE Actors.fname LIKE "D%"
        GROUP BY Actors.aid
        '''
        self.cur.execute(query)

        query = '''
        DROP VIEW IF EXISTS minYear
        '''
        self.cur.execute(query)

        query = '''
        CREATE VIEW minYear AS SELECT c.aid, MIN(m.year) as min_year
        FROM Movies as m
        INNER JOIN Cast as c on m.mid=c.mid
        INNER JOIN startD
        ON c.aid=startD.aid GROUP BY c.aid
        '''
        self.cur.execute(query)

        query = '''
        SELECT startD.fname, startD.lname, count(m.mid) as ct
        FROM startD INNER JOIN Cast as c ON startD.aid=c.aid
        INNER JOIN Movies as m ON c.mid=m.mid INNER JOIN minYear on minYear.aid=c.aid
        WHERE m.year= minYear.min_year
        GROUP BY startD.fname, startD.lname
        ORDER by ct DESC
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q10(self):
        query = '''
        SELECT act.lname, mov.title
        FROM Actors AS act
        INNER JOIN Cast AS c ON act.aid = c.aid
        INNER JOIN Movies AS mov ON c.mid = mov.mid
        INNER JOIN Movie_Director AS md ON c.mid = md.mid
        INNER JOIN Directors AS d ON d.did = md.did
        WHERE act.lname = d.lname
        ORDER BY act.lname, mov.title
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q11(self):
        self.cur.execute('DROP VIEW IF EXISTS baconNumber')
        query = '''
        CREATE VIEW baconNumber as
        SELECT DISTINCT c.aid FROM Cast c
        WHERE c.mid in (SELECT c.mid FROM Cast c WHERE
                            c.aid = (SELECT a.aid FROM Actors a
                            WHERE a.lname  = 'Bacon' and a .fname = 'Kevin'))
        '''
        self.cur.execute(query)

        self.cur.execute('DROP VIEW IF EXISTS baconNumberNOTKevin')
        query = '''
        CREATE VIEW baconNumberNOTKevin as
        SELECT DISTINCT cast1.aid from Cast cast1
        WHERE cast1.mid in (SELECT c.mid FROM Cast c WHERE c.aid in (SELECT * FROM baconNumber))

        '''

        self.cur.execute(query)


        query = '''
        SELECT a1.fname, a1.lname
		FROM baconNumberNOTKevin b, Actors a1
		WHERE b.aid = a1.aid AND b.aid NOT IN (SELECT * FROM baconNumber)
        ORDER BY  a1.lname, a1.fname
		'''

        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q12(self):
        query = '''
        SELECT a.fname, a.lname, COUNT(m.mid), AVG(m.rank) AS p
        FROM Actors AS a INNER JOIN Cast AS c ON a.aid = c.aid
        INNER JOIN Movies AS m ON c.mid = m.mid
        GROUP BY a.aid
        ORDER BY p DESC
        LIMIT 20
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

if __name__ == "__main__":
    task = Movie_db("cs1656-public.db")
    rows = task.q0()
    print(rows)
    print()
    rows = task.q1()
    print(rows)
    print()
    rows = task.q2()
    print(rows)
    print()
    rows = task.q3()
    print(rows)
    print()
    rows = task.q4()
    print(rows)
    print()
    rows = task.q5()
    print(rows)
    print()
    rows = task.q6()
    print(rows)
    print()
    rows = task.q7()
    print(rows)
    print()
    rows = task.q8()
    print(rows)
    print()
    rows = task.q9()
    print(rows)
    print()
    rows = task.q10()
    print(rows)
    print()
    rows = task.q11()
    print(rows)
    print()
    rows = task.q12()
    print(rows)
    print()
