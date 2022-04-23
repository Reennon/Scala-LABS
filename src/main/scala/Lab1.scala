import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.col

import scala.reflect.io.File


object Lab1 extends App {
	def downloadFile(fileURL: String, localFilePath: String): Unit = {
		try {
			val src = scala.io.Source.fromURL(fileURL)
			val out = new java.io.FileWriter(localFilePath)
			out.write(src.mkString)
			out.close()
		} catch {
			case _: java.io.IOException =>
		}
	}

	import org.apache.log4j.{Level, Logger}

	Logger.getLogger("org").setLevel(Level.OFF)
	Logger.getLogger("akka").setLevel(Level.OFF)

	import org.apache.spark.sql.SparkSession

	val fileName: String = "zipcodes.csv"
	val fileURL: String = "https://raw.githubusercontent.com/spark-examples/spark-scala-examples/3ea16e4c6c1614609c2bd7ebdffcee01c0fe6017/src/main/resources/" + fileName
	val localFilePath: String = "/Users/rkovalch/Documents/AIS/Scala/Lab1/src/datasets/" + fileName

	print(!File(localFilePath).exists match {
		case true => {
			downloadFile(
				fileURL = fileURL,
				localFilePath = localFilePath
			)
			s"$fileName is downloaded to $localFilePath"
		}
		case _ => s"$fileName already exists in $localFilePath"
	})

	val spark = SparkSession
		.builder()
		.appName("Spark SQL basic example")
		.config("spark.master", "local")
		.getOrCreate()

	val df = spark
		.read
		.option("header", "true")
		.csv(localFilePath)

	df.select(df.columns.map {
		case column@"RecordNumber" =>
			col(column).cast("int").as(column)
		case column@"Zipcode" =>
			col(column).cast("string").as(column)
		case column@"ZipCodeType" =>
			col(column).cast("string").as(column)
		case column@"City" =>
			col(column).cast("string").as(column)
		case column@"Lat" =>
			col(column).cast("double").as(column)
		case column@"Long" =>
			col(column).cast("double").as(column)
		case column@"Xaxis" =>
			col(column).cast("double").as(column)
		case column@"Yaxis" =>
			col(column).cast("double").as(column)
		case column@"Country" =>
			col(column).cast("string").as(column)
		case column@"LocationText" =>
			col(column).cast("string").as(column)
		case column@"Decommisioned" =>
			col(column).cast("boolean").as(column)
		case column@"TaxReturnFiled" =>
			col(column).cast("int").as(column)
		case column@"EstimatedPopulation" =>
			col(column).cast("int").as(column)
		case column@"TotalWages" =>
			col(column).cast("int").as(column)
		case column@"Notes" =>
			col(column).cast("string").as(column)
		case column =>
			col(column)
	}: _*)
		.na
		.fill(0, Array("EstimatedPopulation", "TotalWages", "TaxReturnsFiled"))
		.withColumn("MeanWage", col("TotalWages") / col("EstimatedPopulation"))
		.write
		.mode(SaveMode.Overwrite)
		.parquet("/Users/rkovalch/Documents/AIS/Scala/Lab1/src/datasets/zipcodes.parquet")


	val parqDF = spark.read.parquet("/Users/rkovalch/Documents/AIS/Scala/Lab1/src/datasets/zipcodes.parquet")
	parqDF.show(false)
	parqDF.printSchema()

}
