import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object T1GroupBy {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder
      .appName("T1GroupBy")
      .master("local[*]")
      .getOrCreate()

    val T1: DataFrame = sc.read
      .option("header", "true")
      .csv("src/main/data/T1.csv")
    T1.createOrReplaceTempView("T1")

    val T1Grouped = sc.sql(
      """
        |SELECT
        |  TransNumItems,
        |  percentile_approx(TransTotal, 0.5) AS Median,
        |  MIN(TransTotal) AS MinAmount,
        |  MAX(TransTotal) AS MaxAmount
        |FROM
        |  T1
        |GROUP BY
        |  TransNumItems
        |  """.stripMargin)

    T1Grouped.show()
    sc.stop()
  }
}