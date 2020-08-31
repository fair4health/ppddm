package ppddm.manager.config

import akka.actor.ActorSystem

object ManagerExecutionContext {
  implicit lazy val actorSystem: ActorSystem = ActorSystem("ppddm-manager")
}
