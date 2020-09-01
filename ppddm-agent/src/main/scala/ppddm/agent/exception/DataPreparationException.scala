package ppddm.agent.exception

final case class DataPreparationException(private val reason: String, private val cause: Throwable = None.orNull)
  extends Exception(reason: String, cause: Throwable) {
}

