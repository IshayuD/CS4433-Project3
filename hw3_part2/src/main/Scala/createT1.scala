import org.apache.spark.sql.{DataFrame, SparkSession}

object createT1 {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder
      .appName("creatT1")
      .master("local[*]")
      .getOrCreate()

    val df: DataFrame = sc.read
      .option("header", "true")
      .csv("src/main/data/purchasesSmall.csv")
    df.createOrReplaceTempView("df")

    val T1 = sc.sql("SELECT * FROM df WHERE TransTotal <= 600")

    T1.coalesce(1)
      .write
      .option("header", "true")
      .csv("src/main/data/T1")

    sc.stop()
  }
}