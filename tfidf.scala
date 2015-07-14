
import java.io._
import org.apache.spark._

object tfidf {
def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }
  def main(args: Array[String]) {
   val file = sc.textFile("s3n://s15-p42-part1-easy/data/")
    val len = file.count()
 val title_text = file.map(_.split("\t")).map { item => item(1) -> item(3).replaceAll("\\<.*?\\>", " ").replace("\\n", " ").replaceAll("\\p{P}", " ").replaceAll("\\p{Digit}", " ").toLowerCase().trim }
 val term_doc = title_text.flatMap(tuple => tuple._2.split("\\s+").map(word => (word, tuple._1)))
 val tf = term_doc.map(item =>item ->1).reduceByKey(_ + _)
   val term_df = term_doc.distinct().mapValues(v=>1).reduceByKey(_+_)
val idf = term_df.mapValues(x => Math.log(len * 1.0 / x))
val idf_cloud  = idf.filter(_._1 =="cloud").values.first()
val cloud = tf.filter(_._1._1=="cloud").sortBy(x => (-x._2, x._1._2)).map(item => item._1._2 ->item._2 *idf_cloud).take(100)

    printToFile(new File("/root/tfidfans.txt")) { p =>
      for (each <- cloud) p.println(each._1+"\t"+each._2)
    }

  }
}
tfidf.main(new Array[String](1))