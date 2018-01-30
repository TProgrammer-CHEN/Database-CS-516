# -*- coding: utf-8 -*-
"""
Created on Fri Sep  8 19:03:34 2017

@author: Iven
"""

import psycopg2;import xml.sax;

conn = psycopg2.connect("dbname=dblp user=dblpuser")
cur = conn.cursor()

cur.execute("CREATE TABLE Authorship (pubkey varchar, author varchar, primary key(pubkey,author));")

class MovieHandler( xml.sax.ContentHandler ):
   def __init__(self):
      self.position = ""
      self.pubkey = ""
      self.author = ""
      self.qual=False
      self.counter=0
      self.set=set([])

   # Call when an element starts
   def startElement(self, tag, attributes):       
      self.position = tag
      if tag == "article" or tag=="inproceedings":
         self.qual=True
         self.pubkey=attributes["key"]

   # Call when an elements ends
   def endElement(self, tag):
      if tag=="author" and self.qual:
          if (self.pubkey,self.author) not in self.set:
              cur.execute("INSERT INTO Authorship (pubkey,author) VALUES (%s, %s)",(self.pubkey,self.author))
              self.set.add((self.pubkey,self.author))
      self.position = ""         
      self.counter=0
      if tag == "article" or tag=="inproceedings":
          self.qual=False


   # Call when a character is read
   def characters(self, content):
       if self.position=="author":
           if self.counter>0:
               self.author=self.author+content
           else:
              self.author=content
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