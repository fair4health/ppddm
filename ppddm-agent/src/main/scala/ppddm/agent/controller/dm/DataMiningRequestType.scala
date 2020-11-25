package ppddm.agent.controller.dm

object DataMiningRequestType extends Enumeration {
  type DataMiningRequestType = String
  val TRAIN = "train"
  val VALIDATE = "validate"
  val TEST = "test"
  val ARL = "arl"
}
