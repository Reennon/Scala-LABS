package DataFormatters

import DataFormatters.DataFrameFormatters.map_complex_id

class DataFormattersTests extends munit.FunSuite {
	test("DataFrameFormatters map_complex_id correctly maps comples id") {
		// Arrange
		val complex_id: String = "HOBBIES_1_001_CA_1_validation"
		val expected_id_array: Array[String] = Array("HOBBIES_1_001", "HOBBIES_1", "HOBBIES", "CA_1", "CA")

		// Act
		val obtained_id_array: Array[String] = map_complex_id(complex_id)

		// Assert
		assertEquals(obtained_id_array.seq, expected_id_array.seq)
	}
}
