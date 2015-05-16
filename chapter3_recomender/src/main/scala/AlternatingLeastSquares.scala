package ccoffey.spark.chapter3

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.recommendation._
import org.apache.spark.rdd.RDD

import scala.collection.immutable.Stream
import scala.util.{Random, Try}

object AlternatingLeastSquares extends java.io.Serializable{

  //this assumes you're running this code from the src/main/scala/ directory
  private val DataDirectory = "../../../data"

  object Tokens extends java.io.Serializable {
    val Tab = '\t'
    val Space = ' '
  }

  def run(sc: SparkContext) = {
    val artists = loadRawArtists(sc)
    val allData = loadUserPlays(sc)

    //pretty terrible model
    //val model = ALS.trainImplicit(allData, 10, 5, 0.01, 1.0)

    val Array(training, cv) = allData.randomSplit(Array(0.9, 0.1))
    training.cache()
    cv.cache()

    val allItemIds = allData.map(_.product).distinct.collect
    val broadcastIds = sc.broadcast(allItemIds)

    val betterModel = ALS.trainImplicit(training, 10, 5, 0.01, 1.0)
    val area = areaUnderCurve(cv, broadcastIds, betterModel.predict)

    area
  }

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext()
    run(sc)
  }

  case class UserArtistRecord(
                             userId: Int,
                             artistId: Int,
                             playCount: Int)

  object UserArtistRecord {
    def parse(s: String) = {
      val ls = s.split(Tokens.Space)
      UserArtistRecord(ls(0).toInt, ls(1).toInt, ls(2).toInt)
    }
  }

  case class ArtistDetails(
                          id: Int,
                          name: String)

  object ArtistDetails {
    val parse = (s: String) =>
    Try {
      val (id, name) = s.span(_ != Tokens.Tab)
      ArtistDetails(id.toInt, name.trim)
    }.toOption
  }

  val loadRawArtists = (sc: SparkContext) => {
    sc.textFile(s"$DataDirectory/artist_data.txt")
      .flatMap(l => {
        ArtistDetails.parse(l)
      })
  }

  val loadArtistAliases = (sc: SparkContext) => {
    sc.textFile(s"$DataDirectory/artist_alias.txt").flatMap(s => {
      s.split(Tokens.Tab).toList match {
        case a :: b :: Nil => Try{
          (a.toInt, b.toInt)
        }.toOption
        case _ => Nil
      }
    }).collectAsMap()
  }

  val loadUserPlays = (sc: SparkContext) => {
    val bArtistAlias = sc.broadcast(loadArtistAliases(sc))

    sc.textFile(s"$DataDirectory/user_artist_data.txt").map(UserArtistRecord.parse).map( rec => {
      val finalArtistId = bArtistAlias.value.getOrElse(rec.artistId, rec.artistId)
      Rating(rec.userId, finalArtistId, rec.playCount)
    })

  }



  def areaUnderCurve(pos: RDD[Rating],
                     itemIds: Broadcast[Array[Int]],
                     prediction: (RDD[(Int, Int)] => RDD[Rating])): Double = {
    val positiveUserProducts = pos.map(r => (r.user, r.product))
    val positivePredictions = prediction(positiveUserProducts).groupBy(_.user)

    val negativeUserProducts = positiveUserProducts.groupByKey.mapPartitions {
      tup => {
        val r = new Random()
        val allItems = itemIds.value
        tup.map {
          case (userId, posItemIds) =>
            val posSet = posItemIds.toSet
            lazy val negatives = Stream.from(0)
              .map(i => allItems(r.nextInt(posSet.size)))
              .filter(!posSet.contains(_))
            negatives.view.take(posSet.size).map(id => (userId, id)).toList
        }
      }
    }.flatMap(t => t)

    val negativePredictions = prediction(negativeUserProducts).groupBy(_.user)
    (positivePredictions join negativePredictions).values.map {
      case (positives, negatives) =>
        val (correct, total) = positives.foldLeft((0L, 0L))((acc, posRating) => {
          negatives.foldLeft(acc)((inner, negRating) =>
            if(posRating.rating > negRating.rating) (inner._1 +1, inner._2 +1) else (inner._1, inner._2 +1)
          )
        })
        correct.toDouble / total
    }.mean
  }


}
