package ppddm.agent.spark

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory

object NodeExecutionContext {
  private lazy val config = ConfigFactory.parseString("akka.daemonic = on")
    .withFallback(ConfigFactory.load())
  implicit lazy val actorSystem: ActorSystem = ActorSystem("worker-node-system", config)
}
