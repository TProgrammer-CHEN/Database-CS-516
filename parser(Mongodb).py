# -*- coding: utf-8 -*-
"""
Created on Sat Nov 18 15:47:24 2017

@author: Iven
"""
import pymongo;import xml.sax

client = pymongo.MongoClient()
db=client.dblp

db.Article.drop()
db.Inproceedings.drop()

class MovieHandler(xml.sax.ContentHandler):
   def __init__(self):
      self.position= ""
      self.pubkey = ""
      self.title=""
      self.journal=""
      self.booktitle=""
      self.year=""
      self.counter=0
      self.authors=[]
      self.author=""
      self.qual=False

   # Call when an element starts
   def startElement(self, tag, attributes):       
      self.position = tag
      if tag == "article" or tag == "inproceedings":
         self.pubkey=attributes["key"]
         self.qual=True
         self.authors=[]
         self.author=""

   # Call when an elements ends
   def endElement(self, tag):
      if self.qual:
          if tag=="author":
              if self.author not in self.authors:
                  self.authors.append(self.author)
          elif tag=="article" :
              db.Article.insert_one({
                        "pubkey":self.pubkey,
                        "title":self.title,
                        "journal":self.journal,
                        "year":self.year,
                        "authors":self.authors})
              self.qual=False
          elif tag=="inproceedings":
              db.Inproceedings.insert_one({
                         "pubkey":self.pubkey,
                         "title":self.title,
                         "booktitle":self.booktitle,
                         "year":self.year,
                         "authors":self.authors})
              self.qual=False
      self.position = ""
      self.counter=0

   # Call when a character is read
   def characters(self, content):
       if self.qual:
           if self.position=="author":
               if self.counter>0:
                   self.author=self.author+content
               else:
                   self.author=content
               self.counter=self.counter+1
           elif self.position=="title":
               if self.counter>0:
                   self.title=self.title+content
               else:
                   self.title=content
               self.counter=self.counter+1
           elif self.position=="year":
               if self.counter>0:
                   self.year=self.year+content
               else:
                   self.year=content
               self.counter=self.counter+1
           elif self.position=="journal":
               self.journal=content
           elif self.position=="booktitle":
               self.booktitle=content
      
if ( __name__ == "__main__"):   
   # create an XMLReader
   parser = xml.sax.make_parser()
   # turn off namepsaces
   parser.setFeature(xml.sax.handler.feature_namespaces, 0)

   # override the default ContextHandler
   Handler = MovieHandler()
   parser.setContentHandler(Handler)
   
   parser.parse("dblp-2017-08-24.xml")
