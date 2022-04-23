package DataFormatters

import DataFormatters.DataSetConstants.datasets_map
import HelperClasses.SparkHelpers.{melt, setup_logging, setup_spark_session}
//import Lab2.sample_submission
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

class HelperClassesTests extends munit.FunSuite {
	test("SparkHelpers sets logging level") {
		// Arrange
		val expected_logging_level: Level = Level.OFF

		// Act
		setup_logging()

		// Assert
		assertEquals(Logger.getLogger("org").getLevel, expected_logging_level)
		assertEquals(Logger.getLogger("akka").getLevel, expected_logging_level)
	}

	test("SparkHelpers creates spark session with given name") {
		// Arrange
		val spark_session_name: String = "Spark SQL basic example"

		// Act
		val spark_session: SparkSession = setup_spark_session(spark_session_name)

		// Assert
		assertEquals(spark_session.conf.get("spark.master"), "local")
	}

	test("SparkHelpers melt melts byte-formatted df") {
		// Arrange
		val expected_columns = Array("id", "variable", "demand")
		val spark_session = setup_spark_session()
		val sample_submission = spark_session
			.read
			.option("header", "true")
			.csv(datasets_map("sample_submission"): String)
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

		// Act
		val melted_sample_submission = melt(
			df=sample_submission,
			id_vars=ss_id_vars,
			value_vars=ss_value_vars,
			value_name="demand"
		)

		// Assert
		assertEquals(melted_sample_submission.columns.toArray.seq, expected_columns.seq)
	}
}
