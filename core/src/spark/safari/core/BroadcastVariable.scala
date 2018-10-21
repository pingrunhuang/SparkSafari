package spark.safari.core

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * A demonstration of how to use broadcast variable
  * - immutable
  * - small enough to fit in mem
  * - distributed to cluster
  *
  * checkin table:
  * | UserId | Neighborhood |
  * |---------+--------------|
  * | 234 | 1 |
  * | 567 | 2 |
  * | 234 | 3 |
  * | 532 | 2 |
  * neighborhoods table:
  * | NeighborhoodId | Name |
  * |----------------+----------------|
  * | 1 | Mission |
  * | 2 | SOMA |
  * | 3 | Sunset |
  * | 4 | Haight Ashbury |
  *
  * checkin is pretty huge while neighborhoods is quite small basic info
  * Using broadcast var could avoid shuffle
  */
class BroadcastVariable(spark: SparkSession) {
  val checkins    = Seq((234, 1), (567, 2), (234, 3), (532, 2))
  val hoods       = Seq((1, "Mission"), (2, "SOMA"), (3, "Sunset"), (4, "Haight Ashbury"))
  val checkinRDD  = spark.sparkContext.parallelize(checkins)
  val hoodsRDD    = spark.sparkContext.parallelize(hoods)

  def join():RDD[(Int, Int, Any)]={
    val broadcastedHoods = spark.sparkContext.broadcast(hoodsRDD.collectAsMap())

//    rdd join
    val checkinsWithHoods = checkinRDD.mapPartitions({row =>
      row.map(x => (x._1, x._2, broadcastedHoods.value.getOrElse(x._2, -1)))
    }, preservesPartitioning = true)
    checkinsWithHoods
}
}
