
import java.io._
import org.apache.spark._

object PageRank {
def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }
  def main(args: Array[String]) {
    val links = sc.textFile("s3n://s15-p42-part2/wikipedia_arcs").map(s => 
      val parts = s.split("\\s+")
      (parts(0), parts(1))
      ).distinct().groupByKey().persist()
    val mapping = sc.textFile("s3n://s15-p42-part2/wikipedia_arcs")
   
    //Total elements in mapping file
    val totalEle = mapping.count()

    //To get the number of dangling elements
     var danglingNum = totalEle - parts.count()

    //RDD of (URL, links) as adjacency List
    var ranks = links.mapValues(v=>1.0)

    val ITERATIONS = 10

    //RDD of (URL, rank) as initial PR values
    for (i<- 1 to ITERATIONS) {
      //Build an RDD of (targetURL, float) pairs
      //with the contributions sent by each page
      val contribs = links.join(ranks).values.flatMap {
        case (urls, rank) =>
        urls.map(dest => (dest, rank / urls.size))
      }
      //Sum contributions by URL and get new ranks
      ranks = contribs.reduceByKey(_+_)
        .mapValues(0.15+0.85*_)
    }

  }
}
P42.main(new Array[String](1))