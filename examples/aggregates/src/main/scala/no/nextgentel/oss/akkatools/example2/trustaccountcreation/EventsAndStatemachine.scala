package no.nextgentel.oss.akkatools.example2.trustaccountcreation

import no.nextgentel.oss.akkatools.aggregate.{AggregateState, AggregateError}

// Events



trait TACEvent

case class TrustAccountCreationInfo(customerNo:String, trustAccountType:String)

case class RegisteredEvent(info:TrustAccountCreationInfo)      extends TACEvent
case class ESigningStartedEvent()                              extends TACEvent
case class ESigningFailedEvent()                               extends TACEvent
case class ESigningCompletedEvent()                            extends TACEvent
case class CreatedEvent(trustAccountId:String)                 extends TACEvent
case class DeclinedEvent(cause:String)                         extends TACEvent

// Generic TAC-Error
case class TACError(e: String) extends AggregateError(e)


object StateName extends Enumeration {
  type StateName = Value
  val NEW = Value("New")
  val REGISTERED = Value("Registered")
  val PENDING_E_SIGNING = Value("Pending_E_Signing")
  val PROCESSING = Value("Processing")
  val DECLINED = Value("Declined")
  val CREATED = Value("Created")
}

import StateName._

object TACState {
  def empty() = TACState(NEW, None, None, None)
}

case class TACState
(
  state:StateName,
  info:Option[TrustAccountCreationInfo],
  trustAccountId:Option[String],
  declineCause:Option[String]
  ) extends AggregateState[TACEvent, TACState] {

  override def transition(event: TACEvent) = {
    (state, event) match {
      case (NEW,               e:RegisteredEvent)        => TACState(REGISTERED, Some(e.info), None, None)
      case (REGISTERED,        e:ESigningStartedEvent)   => copy( state = PENDING_E_SIGNING )
      case (PENDING_E_SIGNING, e:ESigningFailedEvent)    => copy( state = DECLINED, declineCause = Some("E-Signing failed") )
      case (PENDING_E_SIGNING, e:ESigningCompletedEvent) => copy( state = PROCESSING )
      case (PROCESSING,        e:DeclinedEvent)               => copy( state = DECLINED, declineCause = Some(e.cause) )
      case (PROCESSING,        e:CreatedEvent)                => TACState( CREATED, info, Some(e.trustAccountId), None )

      case (s, e:AnyRef) =>
        val eventName = e.getClass.getSimpleName
        throw new TACError(s"Current state is '$s'. Got invalid event: $eventName")
    }
  }
}

