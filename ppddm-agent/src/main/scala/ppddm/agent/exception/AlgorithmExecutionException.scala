package ppddm.agent.exception

final case class AlgorithmExecutionException(private val reason: String, private val cause: Throwable = None.orNull)
  extends Exception(reason: String, cause: Throwable) {
}