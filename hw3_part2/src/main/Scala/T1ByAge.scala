import org.apache.spark.sql.{DataFrame, SparkSession}

object T1ByAge {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder
      .appName("T1ByAge")
      .master("local[*]")
      .getOrCreate()

    val T1: DataFrame = sc.read
      .option("header", "true")
      .csv("src/main/data/T1.csv")
    T1.createOrReplaceTempView("T1")

    val cusDF: DataFrame = sc.read
      .option("header", "true")
      .csv("src/main/data/customersSmall.csv")
    cusDF.createOrReplaceTempView("cusDF")

    val T3 = sc.sql(
      """
        |SELECT
        |  C.ID AS CustID,
        |  C.Age AS Age,
        |  SUM(T1.TransNumItems) AS TotalItems,
        |  SUM(T1.TransTotal) AS TotalSpent
        |FROM
        |  cusDF C
        |JOIN
        |  T1 ON C.ID = T1.CustID
        |WHERE
        |  C.Age BETWEEN 18 AND 25
        |GROUP BY
        |  C.ID, C.Age
        |""".stripMargin)

    T3.coalesce(1)
      .write
      .option("header", "true")
      .csv("src/main/data/T1GroupedByAge")

    sc.stop()
  }
}
