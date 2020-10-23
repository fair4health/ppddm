package ppddm.agent

import akka.actor.ActorSystem
import akka.http.scaladsl.testkit.Specs2RouteTest
import org.apache.spark.sql.SparkSession
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfterAll
import ppddm.agent.config.AgentConfig
import ppddm.core.ai.DataMiningEngine

trait PPDDMAgentTest extends Specification with Specs2RouteTest with BeforeAfterAll {
  override def beforeAll(): Unit = Agent.dataMiningEngine = DataMiningEngine(AgentConfig.appName, AgentConfig.sparkMaster)

  implicit val actorSystem: ActorSystem = ActorSystem("ppddm-agent-test")
  lazy val sparkSession: SparkSession = Agent.dataMiningEngine.sparkSession
}
