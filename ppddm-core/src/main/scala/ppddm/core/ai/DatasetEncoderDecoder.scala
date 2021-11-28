package ppddm.core.ai

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File}
import java.util.Base64

import com.typesafe.scalalogging.Logger
import org.apache.spark.ml.PipelineModel
import org.zeroturnaround.zip.ZipUtil
import ppddm.core.store.DataStoreManager

object DatasetEncoderDecoder {

  protected val logger: Logger = Logger(this.getClass)

  def toString(path: String): String = {
    try {
      val stream = new ByteArrayOutputStream()
      ZipUtil.pack(new File(path), stream)
      val bytes = stream.toByteArray
      stream.close()
      Base64.getEncoder.encodeToString(bytes)
    } catch {
      case e:Exception =>
        val msg = "Error while generating a Base64 encoded string of the given path."
        logger.error(msg, e)
        throw e
    }
  }

  def fromString(modelString: String, path: String): Boolean = {
    try {
      val bytes = Base64.getDecoder.decode(modelString)
      val stream = new ByteArrayInputStream(bytes)
      ZipUtil.unpack(stream, new File(path))
      stream.close()

      true
    } catch {
      case e:Exception =>
        val msg = "Error while creating executing fromString in DatasetEncoderDecoder."
        logger.error(msg, e)
        throw e
    }
  }

}
