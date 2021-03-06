import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object PageRank {
  def main(args: Array[String]){
    val sc = new SparkContext(new SparkConf().setAppName("task1"))
    val factor = 0.85
    val its = 10
    val file = sc.textFile("s3://cmucc-datasets/TwitterGraph.txt")
    val followers = file.flatMap(line => line.split("\t").slice(0, 1)).distinct()
    val followees = file.flatMap(line => line.split("\t").slice(1, 2)).distinct()
    val followeesCount = followees.count()
    var ranks = (followees++followers).distinct().map(word => (word, 1.0))
    val map = file.map(line => line.split("\t")).map(line => (line(0), line(1))).distinct
    val noFollowees = followees.subtract(followers).map(word => (word,""))
    val noFollowersContributes = followers.subtract(followees).map(word => (word, 0.0))
    val noFolloweesCount = noFollowees.count()
    val aggregatedFollowers = (map++noFollowees).groupByKey() // group all followees for one follower
    val aggregatedFollowersCount = aggregatedFollowers.count()
    for( it <- 1 to its){
      //find total weight of people without followees
      val weights = aggregatedFollowers.join(ranks).flatMap{
        case(s, (k, v)) =>
        k.filter { x => x.size == 0 }.map(f => v)
      }
      val weight = weights.reduce((a, b) => a + b)/aggregatedFollowersCount
      val cons = aggregatedFollowers.join(ranks).flatMap{
        case(s, (k, v)) => 
          k.filterNot { x => x.size == 0 }.map(follower => (follower, v/(k.size)))
      }
      val contributes = cons.reduceByKey(_+_).union(noFollowersContributes)
      ranks = contributes.map{
        case(k, v) =>
        val newRank = 0.15 + 0.85 * (v + weight)
        (k ,newRank)
      }
    }
    val output = ranks.map(tuples => s"${tuples._1}\t${tuples._2}")
    output.saveAsTextFile("hdfs:///pagerank-output")
    sc.stop()
  }
}