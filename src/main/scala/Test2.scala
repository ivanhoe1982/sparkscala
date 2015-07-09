import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}
import org.apache.spark.graphx
import org.apache.spark.graphx._
import scala.math._
import org.apache.spark.SparkContext._
import scala.io.Source._


object EditDistance {
  def editDist[A](a: Iterable[A], b: Iterable[A]) =
    ((0 to b.size).toList /: a)((prev, x) =>
      (prev zip prev.tail zip b).scanLeft(prev.head + 1) {
        case (h, ((d, v), y)) => min(min(h + 1, v + 1), d + (if (x == y) 0 else 1))
      }) last

  def relEditDist[A](a: Iterable[A], b: Iterable[A]) =
    1.0f - editDist(a,b).toFloat / max(a.size,b.size)
}

case class Site( id: Long, name : String, long : Double, lat : Double)

object Test2 {
  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
  }

  def normalize(n : Double, max : Double, min : Double): Double = {

    (n - min) / (max - min)
  }

  def distance (s1 : Site, s2 :Site) : Float = {
    //id must be different and all other attributes stay the same
    //if ( (s1.id != s2.id) && (s1.name == s2.name) && (s1.long==s2.long) && (s1.lat == s2.lat) ) 1.0f else 0.0f
    if (s1.id != s2.id)
      EditDistance.relEditDist(s1.name,s2.name) else 0.0f

  }

  def sparkflow(inputFile : String, density : Double,similarityThreshold : Float): Unit ={
    val conf = new SparkConf().setAppName("linkage").setMaster("local")
    val sc = new SparkContext(conf)

    val sites = sc.textFile(inputFile)
      .map(_.split('\t'))
      .map(a => Site(a(0).toInt, a(1),a(4).toDouble,a(5).toDouble)).cache()
      //.saveAsTextFile("E:\\test\\test.txt")

    println("Initial count: " + sites.count())

    val longOrdering = new Ordering[Site]() {
      override def compare(x: Site, y: Site): Int =
        Ordering[Double].compare(x.long, y.long)
    }

    val latOrdering = new Ordering[Site]() {
      override def compare(x: Site, y: Site): Int =
        Ordering[Double].compare(x.lat, y.lat)
    }

    val minLong = sites.min()(longOrdering).long
    val maxLong = sites.max()(longOrdering).long
    val minLat = sites.min()(latOrdering).lat
    val maxLat = sites.max()(latOrdering).lat


    val sitesDuped = sites.map( s => Site(s.id,s.name,
      (normalize(s.long,minLong,maxLong)*density).ceil,
      (normalize(s.lat,minLat,maxLat)*density).ceil)
       ) //just normalize the locations to 0..100 for easy handling and cutting decimals with ceil, manipulating density should produce more or less clusters, but is a basic approach
      //cluster keys are therefore 1 degree squares, codified as: 53 long, 53 lat = square ID 53053
      .map(s => (s.long*density+s.lat, s) )
    .groupByKey()
    .map({ case (k,v) => v cross v }) //generate cross product of all items in one square
    .flatMap(x=>x ) //make each pair an individual member of the RDD
    .map({ case(s1,s2) => (s1,s2,distance(s1,s2))}) //run distance calculation, return tuple3 with 2 objects and distance
    .filter( x => x._3>similarityThreshold)
    .map( x => Edge(x._1.id, x._2.id, x._3)).cache()


    val res = sitesDuped.collect()

    val vertices = VertexRDD(sites.map(s => (s.id,s)))

    val graph = Graph(vertices, sitesDuped)

    val result = graph.collectNeighbors(EdgeDirection.Out).sortByKey(ascending = true)

//    result.foreach({case (aid,a) => {
//      println(aid)
//      a.foreach({case (bid,b)=>println("   "+b)})
//    }})
//
//
//    val result = graph.collectNeighbors(EdgeDirection.Out).flatMapValues()
//    ({case (vid,a) => vid})
//
//    result.foreach()

    println("Found similar: " + res.length)
    println("vertices in graph:" + graph.vertices.count)
    println("edges in graph:" + graph.edges.count)
    println("triplets in graph:" + graph.triplets.count)

  }

  def main (args: Array[String]) {
    val density = 100
    val similarityThreshold=0.8f

    val inputFile = args(0)

    sparkflow(inputFile, density,similarityThreshold)


  }
}



//  def boundaries(sites: List[Site]):(Double,Double,Double,Double)= {
//    println(sites.size)
//    val minLong = sites.minBy(_.long).long
//    val maxLong = sites.maxBy(_.long).long
//    val minLat = sites.minBy(_.lat).lat
//    val maxLat = sites.maxBy(_.lat).lat
//
//    return (minLong,minLat,maxLong,maxLat)
//  }
//  def standardlibflow(inputFile : String, density : Double): Unit = {
//    var sites = sitesFromFile(inputFile)
//
//    println(sites.size)
//
//    val (minLong,minLat,maxLong,maxLat) = boundaries(sites )
//
//    sites = sites.map( s => Site(s.name,(normalize(s.long,minLong,maxLong)*density).ceil,
//      (normalize(s.lat,minLat,maxLat)*density).ceil))
//
//    val groupedSites = sites.groupBy(s => s.long*density+s.lat)
//
//    val dupes = groupedSites.map({ case (k,v) => v cross v })
//      .flatMap(x=>x )
//      .map({ case(s1,s2) => (s1,s2,compare(s1,s2))})
//      .filter( x => x._3==true)
//
//
//    println("foo")
//
//  }

//  def function(x: Int) = x*x
//
//  def sitesFromFile(filePath: String):List[Site]={
//
//    val lines = fromFile(filePath)("UTF-8").getLines()
//    val sites = lines
//      .map(_.split('\t'))
//      .map(a => Site(a(1),a(4).toDouble,a(5).toDouble)).toList
//
//    return sites
//
//  }