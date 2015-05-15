package ccoffey.spark.example.intro

import org.apache.spark.util.StatCounter
import org.apache.spark.SparkContext
import org.apache.spark.rdd._

object Intro {

  object Loader {

    //This can't actually be run because it cannot be serialized by Sparkva
    def makeParsed(raw: RDD[String]) = {
      raw.filter(!_.contains("id_1")).map(parseLine(_, ','))
    }

    //this obviously won't work either
    def rddAsStats(parsed: RDD[Record]) =
      parsed.map(md => md.comparisonScores.map(s => NAStatCounter(0, new StatCounter).add(s)))

    def reducedRdd(nasRdd: RDD[Array[NAStatCounter]]) =
      nasRdd.reduce((l, r) => l.zip(r).map { case (a, b) => a.merge(b) })

    // This is how to partition by characteristic & examine features
    //  val statsm = statsWithMissing(parsed.filter(_.isMatch).map(_.comparisonScores))
    //  val statsn = statsWithMissing(parsed.filter(!_.isMatch).map(_.comparisonScores))
    //  statsm.zip(statsn).map {case (m, n) => (m.missing + n.missing, m.counter.mean - n.counter.mean) }.foreach(println)
  }

  case class Record(
                     idA: Int,
                     idB: Int,
                     comparisonScores: Array[Double],
                     isMatch: Boolean)


  def parseLine(line: String, sep: Char) = {
    val pieces = line.split(sep)
    val i1 = pieces(0).toInt
    val i2 = pieces(1).toInt
    val scores = pieces.slice(2, 11).map(x => if (x == "?") Double.NaN else x.toDouble)
    val matched = pieces(11).toBoolean
    Record(i1, i2, scores, matched)
  }

  //Spark's countByValue is roughly equivalent to creating a histogram
  //The trick is to chain a prior call to Map that contains the histogram's distribution function before, like this
  //  <Your RDD>.map(Histogram Bucketing Function).countByValue
  //  At the end, you'll have a collection of n buckets & the count for each bucket. Exciting stuff
  
  //Spark's RDD[Double] extension methods allow for computing stats, i.e.:
  //  count, mean, stdev, max, min
  //  While not earth-shattering, it is useful to have these numbers at hand, particularly stdev


  case class NAStatCounter(missing: Long, counter: StatCounter) {

    import java.lang.Double.isNaN

    def add(x: Double): NAStatCounter = {
      if (isNaN(x)) NAStatCounter(this.missing + 1, this.counter)
      else {
        counter.merge(x)
        NAStatCounter(this.missing, this.counter)
      }
    }

    def merge(other: NAStatCounter) = {
      counter.merge(other.counter)
      NAStatCounter(this.missing + other.missing, this.counter)
    }

    override def toString = s"stats: ${counter.toString} NaN: $missing Total: ${counter.count + missing}"
  }

  def statsWithMissing(rdd: RDD[Array[Double]]) = {
    val basicStats = rdd.mapPartitions(i => {
      val arr = i.next.map(d => NAStatCounter(0, new StatCounter).add(d))
      i.foreach(as => arr.zip(as).foreach { case (n, d) => n.add(d) })
      Iterator(arr)
    })

    basicStats.reduce((l, r) => l.zip(r).map { case (a, b) => a.merge(b) })

  }


  def nanAsZero(d: Double): Double =
    if (Double.NaN.equals(d)) 0.0 else d

  case class Scored(r: Record, score: Double)

}
