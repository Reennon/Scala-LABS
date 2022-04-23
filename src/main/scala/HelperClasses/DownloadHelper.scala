package HelperClasses

object DownloadHelper {
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
}
