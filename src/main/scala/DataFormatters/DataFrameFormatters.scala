package DataFormatters

object DataFrameFormatters {
	def map_complex_id(complex_id: String): Array[String] = {
		val item_id_store_id = complex_id.split('_')

		Array(
			item_id_store_id.slice(0, 3).mkString("_"),
			item_id_store_id.slice(0, 2).mkString("_"),
			item_id_store_id(0),
			item_id_store_id.slice(3, 5).mkString("_"),
			item_id_store_id(3)
		)
	}
}
