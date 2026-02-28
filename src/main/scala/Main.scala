import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
object Main {
  def main(args: Array[String]): Unit = {
    //TIP Press <shortcut actionId="ShowIntentionActions"/> with your caret at the highlighted text
    // to see how IntelliJ IDEA suggests fixing it.
    val spark = SparkSession.builder()
      .appName("CovidProcessor")
      .config("spark.log.level", "WARN")
      .getOrCreate()

    val clickhouseOptions = Map(
      "host" -> sys.env("CH_HOSTNAME"),
      "protocol" -> "http",
      "http_port" -> sys.env.getOrElse("CH_PORT", "8123"),
      "database" -> sys.env.getOrElse("CH_DATABASE", "default"),
      "user"-> sys.env("CH_USERNAME"),
      "password"-> sys.env("CH_PASSWORD")

    )

    val covidDf = spark.read
      .format("clickhouse")
      .options(clickhouseOptions)
      .option("table", "covid19")
      .load()
      .select("location_key", "date", "new_deceased", "cumulative_confirmed")

    val demoDf = spark.read
      .format("clickhouse")
      .options(clickhouseOptions)
      .option("table", "demographics")
      .load()
      .select("location_key", "population")


    // Расчет витрины зарегистрированных случаев заболеваемости (Начало)
    val maxDateDf = covidDf
      .groupBy("location_key")
      .agg(max("date").as("max_date"))

    val latestCovid = covidDf.alias("c")
      .join(maxDateDf.alias("m"),
        col("c.location_key") === col("m.location_key") &&
          col("c.date") === col("m.max_date")
      )
      .select(
        col("c.location_key"),
        col("c.cumulative_confirmed").as("confirmed")
      )

    // Вычислим соотношение зарегистрированных случаев заболеваемости
    // COVID-19 от общего количества жителей локации.
    // Статистика по всему миру.
    val percentageDmResultDf = latestCovid
      .join(demoDf, Seq("location_key"))
      .filter(col("population") > col("confirmed"))
      .withColumn(
        "percentage",
        (col("confirmed") / col("population")) * 100
      )
      .select("location_key", "confirmed", "population", "percentage")

    percentageDmResultDf.write
      .format("clickhouse")
      .options(clickhouseOptions)
      .option("table", "covid_percentage")
      .option("order_by", "location_key")
      .option("engine", "MergeTree()")
      .mode("overwrite")
      .save()

    // Расчет витрины зарегистрированных случаев заболеваемости (Конец)

    // Расчет витрины по статистике смертности в РФ (Начало)
    val covidDfTask2 = covidDf
      .filter(col("location_key").like("RU_%"))
      .select("location_key", "date", "new_deceased")

    val demoDfTask2 = demoDf.select("location_key", "population")

    val joinedDf = covidDfTask2.alias("c")
      .join(demoDfTask2.alias("d"), Seq("location_key"))
      .withColumn("month", trunc(col("date"), "month"))

    // Вычислим по каждому региону РФ в какой месяц регистрировалось наибольшее
    // Количество смертей от COVID-19
    val dmRuResultDf = joinedDf
      .groupBy("location_key", "month")
      .agg(
        max("population").as("population"),
        sum("new_deceased").as("deceased")
      )
      .withColumn(
        "rn",
        row_number().over(
          Window.partitionBy("location_key")
            .orderBy(col("deceased").desc)
        )
      )
      .filter(col("rn") === 1)
      .select("location_key", "month", "population", "deceased")

    dmRuResultDf.write
      .format("clickhouse")
      .options(clickhouseOptions)
      .option("table", "covid_death_rate_ru")
      .option("order_by", "location_key, month")
      .option("engine", "MergeTree()")
      .mode("overwrite")
      .save()

    // Расчет витрины по статистике смертности в РФ (Конец)

  }
}

