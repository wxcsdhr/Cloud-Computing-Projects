import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Follower {
  def main(args: Array[String]){
    //create spark context
    val sc = new SparkContext(new SparkConf().setAppName("task1"))
    val file = sc.textFile("s3://cmucc-datasets/TwitterGraph.txt")
    val counts = file.distinct.flatMap(line => line.split("\t").slice(1, 2)).map(word => (word,1)).reduceByKey(_+_).map(tuples => s"${tuples._1}\t${tuples._2}") //cited[1]
    counts.saveAsTextFile("hdfs:///follower-output")
   //read in text file and split each document into words
  }
}