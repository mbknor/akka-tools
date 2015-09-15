package no.nextgentel.oss.akkatools.example2.trustaccountcreation

import no.nextgentel.oss.akkatools.aggregate.AggregateCmd

// Commands
trait TACCmd extends AggregateCmd {
}

case class CreateNewTACCmd(id:String, info:TrustAccountCreationInfo) extends TACCmd
case class ESigningFailedCmd(id:String)                              extends TACCmd
case class ESigningCompletedCmd(id:String)                           extends TACCmd
case class TACCompletedCmd(id:String, trustAccountId:String)         extends TACCmd
case class TACDeclined(id:String, cause:String)                      extends TACCmd


