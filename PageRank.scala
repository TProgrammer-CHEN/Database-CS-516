import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Do NOT use different Spark libraries.

object PageRank {

  def main(args: Array[String]) {
    val inputDir = "sample-input"
    val linksFile = inputDir + "/links-simple-sorted.txt"
    val titlesFile = inputDir + "/titles-sorted.txt"
    val numPartitions = 10
    val numIters = 10

    val conf = new SparkConf()
      .setAppName("PageRank")
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
      return edge}
    val outlink=links.flatMap(fromto)

    val titles = sc
      .textFile(titlesFile, numPartitions)
    val nodes=titles.zipWithIndex.mapValues(a=>a+1).map(_.swap)

    /* PageRank */
    val n=titles.count()
    val ids=nodes.map{case(id,tt)=>id}
    val d=0.85
    val outs=outlink.map{case(a,b)=>(a,1)}.reduceByKey((a,b)=>a+b)
    val pr0: Double=100/n
    val inlink=outlink.map(_.swap)
    val noinlink=nodes.cogroup(outlink.map(_.swap)).filter{case(from,(title,to))=>to.isEmpty}.map{case (a,(b,c))=>a}
    val factor= ((1-d)/n)*100
    
    var pr=nodes.map{case (id,tt)=>(id,pr0)}
    val pr_noin=noinlink.map(a=>(a,factor))
    
    for (i <- 1 to 10) {
      val tmp=outlink.join(outs).join(pr).map{case(outid,((inid,out),pr))=>(inid,outid,out,pr)}
      val pr_in=tmp.map{case (inid,outid,out,pr)=>(inid,d*(pr/out))}.reduceByKey((a,b)=>a+b).mapValues(a=>a+factor)
      pr=pr_in.union(pr_noin)
      }
    
    val sum=pr.values.sum()
    pr=pr.map(_.swap).sortByKey(false).map(_.swap)
    val res=nodes.join(pr).map{case(id,(tt,pr))=>(id,tt,(pr/sum)*100)}
    .take(10)
    println("[ PageRanks ]")
    res.foreach(println)
  }
}