

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Do NOT use different Spark libraries.

object NoInOutLink {

  def main(args: Array[String]) {
    val inputDir = "sample-input"
    val linksFile = inputDir + "/links-simple-sorted.txt"
    val titlesFile = inputDir + "/titles-sorted.txt"
    val numPartitions = 10

    val conf = new SparkConf()
      .setAppName("NoInOutLink")
      .setMaster("local[*]")
      .set("spark.driver.memory", "1g")
      .set("spark.executor.memory", "2g")

    val sc = new SparkContext(conf)

    val links = sc
      .textFile(linksFile, numPartitions)
      
    def fromto(str:String):List[(Long,Long)]={
      val tnf= str.split(": ",2)
      val from= tnf(0).toLong
      val tos= tnf(1).split(" ")
      var edge:List[(Long,Long)]=List()
      for(to <- tos){edge=edge:+ (from,to.toLong)}
      return edge
  }
    
    val edges=links.flatMap(fromto)

    val titles = sc
      .textFile(titlesFile, numPartitions)
    val nodes=titles.zipWithIndex.mapValues(a=>a+1).map(_.swap)

    /* No Outlinks */
    val noOutlinks=nodes.cogroup(edges).filter{case(from,(title,to))=>to.isEmpty}
    .map{case (a,(b,c))=>(a,b.min)}.sortByKey(true).takeOrdered(10)
    println("[ NO OUTLINKS ]")
    for(i<-noOutlinks){println(i)}
    

    /* No Inlinks */
    val noInlinks=nodes.cogroup(edges.map(_.swap)).filter{case(from,(title,to))=>to.isEmpty}
    .map{case (a,(b,c))=>(a,b.min)}.sortByKey(true).takeOrdered(10)
    println("\n[ NO INLINKS ]")
    for(i<-noInlinks){println(i)}
  }
}