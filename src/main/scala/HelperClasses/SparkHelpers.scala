package HelperClasses

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{array, col, explode, lit, struct}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkHelpers {
	def setup_logging(): Unit = {
		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF)
	}

	def setup_spark_session(name: String = "Spark SQL basic example"): SparkSession = {
		import org.apache.spark.sql.SparkSession

		val spark = SparkSession
			.builder()
			.appName(name)
			.config("spark.master", "local")
			.getOrCreate()

		spark
	}

	def melt(
		df: DataFrame,
        id_vars: Seq[String],
		value_vars: Seq[String],
        var_name: String = "variable",
		value_name: String = "value"
	) : DataFrame = {

        // Create array<struct<variable: str, value: ...>>
        val _vars_and_vals = array((for (c <- value_vars) yield { struct(lit(c).alias(var_name), col(c).alias(value_name)) }): _*)

        // Add to the DataFrame and explode
        val _tmp = df.withColumn("_vars_and_vals", explode(_vars_and_vals))

        val cols = id_vars.map(col _) ++ { for (x <- List(var_name, value_name)) yield { col("_vars_and_vals")(x).alias(x) }}

        return _tmp.select(cols: _*)

    }
}
