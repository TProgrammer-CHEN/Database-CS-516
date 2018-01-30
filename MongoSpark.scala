import org.apache.log4j.{Logger, Level}

import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.rdd.RDD

import 
org.apache.hadoop.conf.Configuration
import 
org.bson.BSONObject

import 
com.mongodb.{
    MongoClient,
    MongoException,
    WriteConcern,
    DB,
    DBCollection,
    BasicDBObject,
    BasicDBList,
    DBObject,
    DBCursor
}


import com.mongodb.hadoop.{
    MongoInputFormat,
    MongoOutputFormat,
    BSONFileInputFormat,
    BSONFileOutputFormat
}


import com.mongodb.hadoop.io.MongoUpdateWritable


object MongoSpark {
    
	def main(args: Array[String]) {
     System.setProperty("hadoop.home.dir", "C:\\Serious\\tmp");   
	/* Uncomment to turn off Spark logs */
        //Logger.getLogger("org").setLevel(Level.OFF)
        //Logger.getLogger("akka").setLevel(Level.OFF)

        val conf = new SparkConf()
            .setMaster("local[*]")
            .setAppName("MongoSpark")
            .set("spark.driver.memory", "2g")
            .set("spark.executor.memory", "4g")

        val sc = new SparkContext(conf)

        val article_input_conf = new Configuration()
        article_input_conf.set("mongo.input.uri", "mongodb://localhost:27017/dblp.Article")

        val inproceedings_input_conf = new Configuration()
        inproceedings_input_conf.set("mongo.input.uri", "mongodb://localhost:27017/dblp.Inproceedings")

        val article = sc.newAPIHadoopRDD(
            article_input_conf,         // config
            classOf[MongoInputFormat],  // input format
            classOf[Object],            // key type
            classOf[BSONObject]         // val type
        )

        val inproceedings = sc.newAPIHadoopRDD(
            inproceedings_input_conf,
            classOf[MongoInputFormat],
            classOf[Object],
            classOf[BSONObject]
        )

        // question numbers correspond to those in HW1
        q2(inproceedings)
        q3b(inproceedings)
        q3d(article, inproceedings)
        q4b(article, inproceedings)
    }

    /* Q2.
     * Add a column "Area" in the Inproceedings table.
     * Then, populate the column "Area" with the values from the above table if
     * there is a match, otherwise set it to "UNKNOWN" */
    def q2(inproceedings: RDD[(Object,BSONObject)]) {
        val Database=List("SIGMOD Conference","VLDB","ICDE","PODS")
        val Theory=List("STOC","FOCS","SODA","ICALP")
        val Systems=List("SIGCOMM","ISCA","PLDI","HPCA")
        val MLAI=List("ICML","NIPS","AAAI","IJCAI")
        val updates = inproceedings.mapValues(
            value =>
              if(Database.contains(value.get("booktitle").asInstanceOf[String])){
              new MongoUpdateWritable(
      new BasicDBObject("_id", value.get("_id")),  // Query
      new BasicDBObject("$set", new BasicDBObject("area", "Database")),
      false,false,false
      )}
              else if(Theory.contains(value.get("booktitle").asInstanceOf[String])){
                new MongoUpdateWritable(
      new BasicDBObject("_id", value.get("_id")),  // Query
      new BasicDBObject("$set", new BasicDBObject("area", "Theory")),
      false,false,false
      )}
              else if(Systems.contains(value.get("booktitle").asInstanceOf[String])){
                new MongoUpdateWritable(
      new BasicDBObject("_id", value.get("_id")),  // Query
      new BasicDBObject("$set", new BasicDBObject("area", "Systems")),
      false,false,false
      )}
              else if(MLAI.contains(value.get("booktitle").asInstanceOf[String])){
                new MongoUpdateWritable(
      new BasicDBObject("_id", value.get("_id")),  // Query
      new BasicDBObject("$set", new BasicDBObject("area", "ML-AI")),
      false,false,false
      )}
              else{
                new MongoUpdateWritable(
      new BasicDBObject("_id", value.get("_id")),  // Query
      new BasicDBObject("$set", new BasicDBObject("area", "UNKNOWN")),
      false,false,false
      )})
      
      val outputConfig = new Configuration()
        outputConfig.set("mongo.output.uri",
        "mongodb://localhost:27017/dblp.Inproceedings")
      updates.saveAsNewAPIHadoopFile(
      "file:///this-is-completely-unused",
      classOf[Object],
      classOf[MongoUpdateWritable],
      classOf[MongoOutputFormat[Object, MongoUpdateWritable]],
      outputConfig)   
    }

    /* Q3b.
     * Find the TOP­20 authors who published the most number of papers in
     * "Database" (author published in multiple areas will be counted in all those
     * areas).
     * Divesh Srivastava
Surajit Chaudhuri
Jiawei Han 0001
Jeffrey F. Naughton
Philip S. Yu
Hector Garcia-Molina
H. V. Jagadish
Raghu Ramakrishnan
Beng Chin Ooi
Michael Stonebraker
Kian-Lee Tan
Rakesh Agrawal
Michael J. Carey
Nick Koudas
David J. DeWitt
Michael J. Franklin
Christos Faloutsos
Dan Suciu
Gerhard Weikum
Jeffrey Xu Yu*/
    def q3b(inproceedings: RDD[(Object,BSONObject)]) {
        val dbat=inproceedings.values
        .filter{v=>
            val area=v.get("area").asInstanceOf[String]
            area=="Database"}
        val authors=dbat.flatMap{case(value)=>
          value.get("authors").asInstanceOf[BasicDBList].toArray}
        
        val authorcount=authors
      .map(author => (author, 1))
      .reduceByKey((a, b) => a + b)
      
      val res=authorcount
      .takeOrdered(20)(Ordering[Int].reverse.on(x => x._2)).map{case(author,count)=>author}
          
      res.foreach(println)
 }


    /* Q3d.
     * Find the number of authors who wrote more journal papers than conference
     * papers (irrespective of research areas).
     *  801767*/
    def q3d(article: RDD[(Object,BSONObject)], inproceedings: RDD[(Object,BSONObject)]) {
        val i_authors=inproceedings.values.flatMap{case(value)=>
          value.get("authors").asInstanceOf[BasicDBList].toArray}
        val a_authors=article.flatMap{case(id,value)=>
          value.get("authors").asInstanceOf[BasicDBList].toArray}
        
        val i_count=i_authors
      .map(author => (author, 1))
      .reduceByKey((a, b) => a + b)
      
        val a_count=a_authors
      .map(author => (author, 1))
      .reduceByKey((a, b) => a + b)
      
        val res=a_count.leftOuterJoin(i_count).mapValues{case(n1,Some(n2))=>(n1,n2)
        case(n1,None)=>(n1,0)}.filter{v=>v._2._1>v._2._2}.keys.count()
        
        println(res)
    }

    /* Q4b.
     * Plot a barchart showing how the average number of collaborators varied in
     * these decades for conference papers in each of the four areas in Q3.
     * Again, the decades will be 1950-1959, 1960-1969, ...., 2000-2009, 2010-2015.
     * But for every decade, there will be four bars one for each area (do not
     * consider UNKNOWN), the height of the bars will denote the average number of
     * collaborators. 
     * (1960,ML-AI,2.3246753246753247)
			 (1960,Theory,2.1000000000000000)(1970,Database,5.8032036613272311)(1970,ML-AI,4.0141752577319588)
			 (1970,Systems,4.5393700787401575)(1970,Theory,4.9471544715447154)(1980,Database,8.5821086261980831)
			 (1980,ML-AI,5.5522448979591837)(1980,Systems,7.0691280439905734)(1980,Theory,9.6827906976744186)
			 (1990,Database,14.3890686803308163)(1990,ML-AI,11.1811100292112950)(1990,Systems,14.9961283185840708)
			 (1990,Theory,15.7752136752136752)(2000,Database,29.7697650663942799)(2000,ML-AI,25.9065554231227652)
			 (2000,Systems,30.5895691609977324)
			 (2000,Theory,25.5415540540540541)(2010,Database,54.6953358208955224)(2010,ML-AI,47.3926075184182356)
			 (2010,Systems,53.3570840197693575)
     * */
    def q4b(article: RDD[(Object,BSONObject)], inproceedings: RDD[(Object,BSONObject)]) {
        val inp=inproceedings
        .map{case(id,value)=>
          (   value.get("pubkey").asInstanceOf[String],
              value.get("year").asInstanceOf[Int],
              value.get("authors").asInstanceOf[BasicDBList])}
        .flatMap{case(pubkey,year,authors)=>
          authors.toArray.map(author=>((pubkey,author.asInstanceOf[String]),year))}
        .mapValues{case(year)=>
          if(year>=1950 & year<=1959){"1950-1959"}
          else if(year>=1960 & year<=1969){"1960-1969"}
          else if(year>=1970 & year<=1979){"1970-1979"}
          else if(year>=1980 & year<=1989){"1980-1989"}
          else if(year>=1990 & year<=1999){"1990-1999"}
          else if(year>=2000 & year<=2009){"2000-2009"}
          else if(year>=2010){"2010-"}
          }.mapValues(v=>v.toString()).map{case((pubkey,author),decade)=>(pubkey,(author,decade))}.cache()
        
        val arp=article
        .map{case(id,value)=>
          (value.get("pubkey").asInstanceOf[String],
              value.get("authors").asInstanceOf[BasicDBList])
          }
        .flatMap{case(pubkey,authors)=>
          authors.toArray.map(author=>(pubkey,author.asInstanceOf[String]))}
        
        val autdecade=inp.map{case(pubkey,(author,decade))=>(pubkey,author)}.union(arp)

        val ncoldecade=inp.join(autdecade).filter{case(pubkey,((author1,decade),author2))=>author1!=author2}
        .map{case(pubkey,((author1,decade),author2))=>((author1,decade),author2)}
        .distinct()
        .map{case((author1,decade),author2)=>((author1,decade),1)}
        .reduceByKey((a,b)=>a+b)
        //.map{case((author1,decade),num)=>(author1,decade,num)}
        
        val autarea=inproceedings
        .map{case(id,value)=>
          (value.get("authors").asInstanceOf[BasicDBList].toArray,
              value.get("area").asInstanceOf[String],value.get("year").asInstanceOf[Int])
          }.filter{case(authors,area,year)=>area!="UNKNOWN"}
        .flatMap{case(authors,area,year)=>
          authors.toArray.map(author=>((author.asInstanceOf[String],area),year))}
        .mapValues{case(year)=>
          if(year>=1950 & year<=1959){"1950-1959"}
          else if(year>=1960 & year<=1969){"1960-1969"}
          else if(year>=1970 & year<=1979){"1970-1979"}
          else if(year>=1980 & year<=1989){"1980-1989"}
          else if(year>=1990 & year<=1999){"1990-1999"}
          else if(year>=2000 & year<=2009){"2000-2009"}
          else if(year>=2010){"2010-"}
          }.mapValues(v=>v.toString()).distinct.map{case((author,area),decade)=>((author,decade),area)}
          
        val avg=autarea.join(ncoldecade).map{case((author,decade),(area,num))=>((decade,area),num)}
        .aggregateByKey((0,0))((acc, value) => (acc._1 + value, acc._2 + 1),(acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
        .mapValues(x => (x._1/x._2))
        
        avg.collect.foreach(println)
    }
}