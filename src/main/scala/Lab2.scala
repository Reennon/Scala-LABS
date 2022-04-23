import DataFormatters.DataFrameFormatters.map_complex_id
import DataFormatters.DataSetConstants.datasets_map
import HelperClasses.SparkHelpers.{melt, setup_logging, setup_spark_session}
import org.apache.spark.sql.functions.{col, date_add, lit, udf}

object Lab2 extends App {
	setup_logging()
	val spark_session = setup_spark_session()
	import spark_session.implicits._

	val calendar_df = spark_session
		.read
		.option("header", "true")
		.csv(datasets_map("calendar"): String)

	val sales_train_evaluation = spark_session
		.read
		.option("header", "true")
		.csv(datasets_map("sales_train_evaluation"): String)

	val sample_submission = spark_session
		.read
		.option("header", "true")
		.csv(datasets_map("sample_submission"): String)

	val sell_prices = spark_session
		.read
		.option("header", "true")
		.csv(datasets_map("sell_prices"): String)

	val reorderedColumnNames: Array[String] = Array("date", "id", "item_id", "dept_id", "cat_id", "store_id", "state_id")

	val ss_id_vars = Array("id")
	val ss_value_vars = sample_submission
		.columns
		.toSeq
		.toSet
		.filterNot(
			ss_id_vars.toSet
		)
		.toSeq
		.map(_.toString)

	val melted_sample_submission = melt(
		df=sample_submission,
		id_vars=ss_id_vars,
		value_vars=ss_value_vars,
		value_name="demand"
	)

	def get_fdays(fday: String) = fday.substring(1, fday.length).toInt
	val getFDays = udf(get_fdays _)
	val getComplexId = udf(map_complex_id _)

	val ss_with_start_date_df = melted_sample_submission
		.withColumn("start_date", lit("2016-05-23"))
		.withColumn("days", getFDays(col("variable")))

	val ss_with_date_unpacked_id_df = ss_with_start_date_df
		.withColumn("date", date_add($"start_date", $"days"))
		.withColumn("item_id", getComplexId(col("id")).getItem(0))
		.withColumn("dept_id", getComplexId(col("id")).getItem(1))
		.withColumn("cat_id", getComplexId(col("id")).getItem(2))
		.withColumn("store_id", getComplexId(col("id")).getItem(3))
		.withColumn("state_id", getComplexId(col("id")).getItem(4))

	val ss_dropped_df = ss_with_date_unpacked_id_df.drop("variable", "start_date", "days")
	val ss_reordered_df = ss_dropped_df
		.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)

	ss_reordered_df.show(false)

	val ste_id_vars = Array("id", "item_id", "dept_id", "cat_id", "store_id", "state_id")
	val ste_value_vars = sales_train_evaluation
		.columns
		.toSeq
		.toSet
		.filterNot(
			ste_id_vars.toSet
		)
		.toSeq
		.map(_.toString)

	val melted_sales_train_evaluation_df = melt(
		df=sales_train_evaluation,
		id_vars=ste_id_vars,
		value_vars=ste_value_vars,
		value_name="demand"
	)
	print(melted_sample_submission.columns.mkString)

	def get_days(dday: String) = dday.substring(2, dday.length).toInt
	val getDDays = udf(get_days _)

	val ste_with_start_date = melted_sales_train_evaluation_df
		.withColumn("start_date", lit("2011-01-01"))
		.withColumn("days", getDDays(col("variable")))

	val ste_with_date_df = ste_with_start_date
		.withColumn("date", date_add($"start_date", $"days"))

	val ste_dropped_df = ste_with_date_df
		.drop("variable", "start_date", "days")
	val ste_reordered_df = ste_dropped_df
		.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)

	ste_reordered_df.show(false)

	val united_df = ste_reordered_df
		.union(ss_reordered_df)

	united_df
		.sort(
			col("date").desc)
		.show(false)

	val united_calendar_df = united_df
		.join(
			calendar_df,
			Seq("date")
		)
	united_calendar_df.show(false)

	val united_calendar_prices_df = united_calendar_df.limit(10)
		.join(
			sell_prices,
			Seq(
				"item_id",
				"store_id",
				"wm_yr_wk"
			)
		)

	united_calendar_prices_df.show(true)
}