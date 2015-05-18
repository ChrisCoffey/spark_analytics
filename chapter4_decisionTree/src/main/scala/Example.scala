package ccoffey.spark.chapter4

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.rdd._

import java.io.{Serializable => JSer}

import org.apache.spark.rdd.RDD

object Example extends JSer{
  private val dataPath = "../../../data"


  def main(args: Array[String]): Unit = {
    val sc = new SparkContext()
    run(sc)
  }

  def run(sc: SparkContext) = {
    //step 1: set up data & partition it
    val data = covData(sc)

    val Array(training, crossValidation, test) = dataSets(data)
    training.cache
    crossValidation.cache
    test.cache

    val initialMetrics = evaluateModel(model1(training), crossValidation)
    val tP = classProbabilities(training)
    val cP = classProbabilities(crossValidation)

    println("============== Training Probabilities ============")
    tP.foreach(println)
    println("============== Cross Validation Probabilities ============")
    cP.foreach(println)

    val totalProbability = tP.zip(cP).map(a => a._1 * a._2).sum
    println("============== Total Probability ============")
    println(totalProbability) // This is roughly what random guessing should achieve

//    val evaluations = tuneModel(training, crossValidation)
  //  evaluations.sortBy(_._2).reverses

    val tuned = tunedModel(training.union(crossValidation))
    evaluateModel(tuned, test)

  }

  def covData(sc: SparkContext) = {
    val raw = sc.textFile(s"$dataPath/covtype.data.csv")
    raw.map { line =>
      val values = line.split(',').map(_.toDouble)
      LabeledPoint(values.last-1, Vectors.dense(values.init))
    }
  }

  def dataSets(data: RDD[LabeledPoint]): Array[RDD[LabeledPoint]] =
    data.randomSplit(Array(0.8, 0.1, 0.1))

  private val Gini = "gini"
  private val Entropy = "entropy"
  private val Auto = "auto"

  def model1(data: RDD[LabeledPoint]) =
    DecisionTree.trainClassifier(data, 7, Map.empty[Int, Int], Gini, 4, 100)

  def tunedModel(data: RDD[LabeledPoint]) =
    DecisionTree.trainClassifier(data, 7, Map.empty[Int, Int], Entropy, 20, 300)

  def forestModel(data: RDD[LabeledPoint]) =
    RandomForest.trainClassifier(data, 7, Map.empty[Int, Int], 20, Auto, Entropy, 30, 300)

  //takes a minute or so locally, but pretty good way for finding the best parameters
  def tuneModel(training: RDD[LabeledPoint], cv: RDD[LabeledPoint]) = {
    for {
      impurity <- Array(Gini, Entropy)
      depth <- Array(1, 20)
      bins <- Array(10, 300)
    } yield {
      val model = DecisionTree.trainClassifier(training, 7, Map.empty[Int, Int], impurity, depth, bins)
      val accuracy = evaluateModel(model, cv).precision
      ((impurity, depth, bins), accuracy)
    }
  }

  def evaluateModel(model: DecisionTreeModel, data: RDD[LabeledPoint]) = {
    new MulticlassMetrics( data.map(a => (model.predict(a.features), a.label)))
  }

  def categoricalPrecision(metrics: MulticlassMetrics) = {
    (0 until 7).map(c => (metrics.precision(c), metrics.recall(c)))
  }

  //Labeled Point is really a fancy Tuple (Array[Double], Double)

  def classProbabilities(data: RDD[LabeledPoint]) = {
    val categoryCounts = data.map(_.label).countByValue
    val counts = categoryCounts.toArray.sortBy(_._1).map(_._2)
    counts.map(_.toDouble / counts.sum)
  }



}
