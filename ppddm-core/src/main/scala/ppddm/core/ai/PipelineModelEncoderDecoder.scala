package ppddm.core.ai

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File}
import java.util.Base64

import com.typesafe.scalalogging.Logger
import org.apache.spark.ml.PipelineModel
import ppddm.core.store.DataStoreManager
import org.zeroturnaround.zip.ZipUtil

object PipelineModelEncoderDecoder {

  protected val logger: Logger = Logger(this.getClass)

  /**
   * Generate a Base64 encoded string of the fitted model of this algorithm
   *
   * @param model A PipelineModel
   * @return
   */
  def toString(model: PipelineModel, path: String): String = {
    try {
      model.save(path)

      val stream = new ByteArrayOutputStream()
      ZipUtil.pack(new File(path), stream)
      val bytes = stream.toByteArray
      stream.close()
      val modelString = Base64.getEncoder.encodeToString(bytes)

      DataStoreManager.deleteDirectoryAsync(path)

      modelString
    } catch {
      case e:Exception =>
        val msg = "Error while generating a Base64 encoded string of the trained model."
        logger.error(msg, e)
        throw e
    }
  }

  /**
   * Creates a PipelineModel from the Base64 encoded fitted_model string of this algorithm.
   *
   * @param modelString Base64 encoded string representation of the PipelineModel file content
   * @return A PipelineModel
   */
  def fromString(modelString: String, path: String): PipelineModel = {
    try {
      val bytes = Base64.getDecoder.decode(modelString)
      val stream = new ByteArrayInputStream(bytes)
      ZipUtil.unpack(stream, new File(path))
      val pipelineModel = PipelineModel.load(path)
      stream.close()

      DataStoreManager.deleteDirectoryAsync(path)

      pipelineModel
    } catch {
      case e:Exception =>
        val msg = "Error while creating PipelineModel from the Base64 encoded fitted_model."
        logger.error(msg, e)
        throw e
    }
  }

}
