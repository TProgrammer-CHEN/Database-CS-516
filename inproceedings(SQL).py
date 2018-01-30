# -*- coding: utf-8 -*-
"""
Created on Fri Sep  8 16:35:27 2017

@author: Iven
"""

import psycopg2;import xml.sax

conn = psycopg2.connect("dbname=dblp user=dblpuser")
cur = conn.cursor()

cur.execute("CREATE TABLE Inproceedings (pubkey varchar PRIMARY KEY, title varchar, booktitle varchar, year integer);")

class MovieHandler( xml.sax.ContentHandler ):
   def __init__(self):
      self.position= ""
      self.pubkey = ""
      self.title=""
      self.booktitle=""
      self.year=""
      self.counter=0

   # Call when an element starts
   def startElement(self, tag, attributes):       
      self.position = tag
      if tag == "inproceedings":
         self.pubkey=attributes["key"]

   # Call when an elements ends
   def endElement(self, tag):
      if tag=="inproceedings" :
          cur.execute("INSERT INTO Inproceedings (pubkey,title,booktitle,year) VALUES (%s, %s, %s, %s)",(self.pubkey,self.title,self.booktitle,self.year))
      self.position = ""
      self.counter=0

   # Call when a character is read
   def characters(self, content):
       if self.position=="title":
           if self.counter>0:
               self.title=self.title+content
           else:
               self.title=content
           self.counter=self.counter+1
       elif self.position=="booktitle":
           self.booktitle=content
       elif self.position=="year":
           if self.counter>0:
               self.year=self.year+content
           else:
               self.year=content
           self.counter=self.counter+1
      
if ( __name__ == "__main__"):   
   # create an XMLReader
   parser = xml.sax.make_parser()
   # turn off namepsaces
   parser.setFeature(xml.sax.handler.feature_namespaces, 0)

   # override the default ContextHandler
   Handler = MovieHandler()
   parser.setContentHandler( Handler )
   
   parser.parse("dblp-2017-08-24.xml")

conn.commit()
cur.close()
conn.close()