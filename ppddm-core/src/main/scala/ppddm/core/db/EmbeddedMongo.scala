package ppddm.core.db

import com.typesafe.scalalogging.Logger
import de.flapdoodle.embed.mongo.{MongodExecutable, MongodStarter}
import de.flapdoodle.embed.mongo.config.{IMongodConfig, MongodConfigBuilder, Net, Storage}
import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.process.runtime.Network

/**
 * Embedded MongoDB Server
 */
object EmbeddedMongo {

  private val logger: Logger = Logger(this.getClass)

  val FOLDER_EXT: String = ".emb.mongo"

  private var mongodExecutable: MongodExecutable = _

  def start(appName:String, host: String, port: Int): Unit = {

    val conf: IMongodConfig = new MongodConfigBuilder()
      .version(Version.Main.PRODUCTION)
      .replication(new Storage(s"./${appName.filterNot(_.isWhitespace)}${FOLDER_EXT}",null,0))
      .net(new Net(host, port, Network.localhostIsIPv6()))
      .build()

    // TODO: Redirect the Mongod runtime logs to the logger using the IRuntimeConfig
    // TODO: Look at how they implemented IStreamProcessor for Slf4j Logger
    // http://flapdoodle-oss.github.io/de.flapdoodle.embed.mongo/
    // https://www.programcreek.com/java-api-examples/?api=de.flapdoodle.embed.mongo.Command

    mongodExecutable = MongodStarter.getDefaultInstance.prepare(conf)

    mongodExecutable.start()
    logger.info("Embedded Mongo is listening on {}:{}", host, port)
  }

  def stop(): Unit = {
    mongodExecutable.stop()
  }

}

