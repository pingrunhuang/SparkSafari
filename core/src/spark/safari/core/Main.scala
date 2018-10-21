package spark.safari.core

import org.apache.spark.sql.SparkSession
/**
  * @ author Frank Huang (runping@shanshu.ai)
  * @ date 2018/10/21 
  */
object Main {
  def main(args: Array[String]): Unit = {
    val master = args(0)

    val sparkSession = SparkSession.
      builder().
      appName("SparkSafari").
      master(master).
      getOrCreate()

    val result = new BroadcastVariable(sparkSession)

    result.join().take(5).foreach(println)
  }
}
