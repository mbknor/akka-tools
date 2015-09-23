package no.nextgentel.oss.akkatools.camel

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.camel._
import akka.pattern.ask
import akka.util.Timeout
import no.nextgentel.oss.akkatools.logging.HasMdcInfo
import no.nextgentel.oss.akkatools.persistence.DurableMessageLike

case class CamelChannelConfig(inboundEndpoint:String, outboundEndpoint:String)

case class CamelIOActorReady private[camel] ()
case class MustConfirmDM private[camel] (dm:DurableMessageLike)

/**
 * When not using autoAckWhenReceiving, messageDest must send back akka.camel.Ack to ack the message
 * @param channel
 * @param messageDest
 * @param autoAckWhenReceiving
 */
abstract class CamelIOActor(channel: CamelChannelConfig, messageDest:ActorRef, autoAckWhenReceiving:Boolean = true) extends Consumer with DiagnosticActorLogging {
  def endpointUri = channel.inboundEndpoint
  import context._

  // Create the actor that will handle the camel-sending
  val senderActor = context.actorOf(Props(classOf[CamelSenderActor], channel.outboundEndpoint), "CamelIOActor_senderActor")
  implicit val timeout = Timeout(60, TimeUnit.SECONDS)

  var earlyMessages:List[Any] = List()


  // Wait for camel to be ready
  camel.activationFutureFor(self).map {
    x =>
      self ! CamelIOActorReady()
  }

  become(initialReceive)

  override def autoAck = autoAckWhenReceiving

  def initialReceive: Receive = {
    case CamelIOActorReady() =>
      log.info(s"Ready to receive messages from camel. channel: $channel")
      become(receive)
      //send pending messages
      earlyMessages.foreach { self ! _}
      earlyMessages = List()
    case msgToSend:Any =>
      earlyMessages = earlyMessages :+ msgToSend
  }

  override def receive = {
    case MustConfirmDM(dm)             =>
      extractAndSetMdc(dm.payload())
      log.debug("Confirming durableMessage")
      dm.confirm(context, self)
    case camelMsg:CamelMessage         => onMessageReceived(camelMsg)
    case dm:DurableMessageLike         => internalDoSendMessage(dm.payload(), () => self ! MustConfirmDM(dm) )
    case x:Any                         => internalDoSendMessage(x, () => Unit )
  }


  def onMessageReceived(camelMsg:CamelMessage): Unit = {

    val msg:Any = try {
      deserialize(camelMsg)
    } catch {
      case e:Exception =>
        onDeserializeError(camelMsg, e)
        // If we're not running with auto-acking, we must ack it manully here
        if(!autoAckWhenReceiving) {
          sender ! Ack
        }
    }

    extractAndSetMdc(msg)
    log.info("Received from " + endpointUri + ": " + msg)

    if (autoAckWhenReceiving) {
      messageDest ! msg
    } else {
      messageDest.forward(msg)
    }


  }
  def extractAndSetMdc(message:Any):Unit = {
    message match {
      case m:HasMdcInfo =>
        log.mdc( log.mdc ++ m.extractMdcInfo())
      case _ => None
    }
  }

  def internalDoSendMessage(msg:Any, executeWhenDelivered: () => Unit): Unit = {
    extractAndSetMdc(msg)
    val msgToSend = serialize(msg)
    log.info("Sending to " + channel.outboundEndpoint+": " + msgToSend)
    ask(senderActor, msgToSend).onSuccess {
      case ack:Any =>
        executeWhenDelivered.apply()
    }
  }

  def deserialize(camelMessage:CamelMessage):Any
  def serialize(message:Any):Any // Can be String or CamelMessage

  def onDeserializeError(camelMsg:CamelMessage, error:Exception): Unit = {
    log.warning("Unable deserialize: " + camelMsg + ". Error: " + error.getMessage)
  }

}


/**
 * Sends all received camelMessages to the configured camel-endpoint.
 * When camel confirms the delivery, we send akka.camel.Ack back to the original sender.
 *
 * This makes it possible to use akka Ask-pattern to get confirmation that the message has been delivered to camel
 * @param endpoint
 */
class CamelSenderActor private [camel] (endpoint:String) extends Producer with Oneway with ActorLogging {
  def endpointUri = endpoint

  override protected def routeResponse(msg: Any): Unit = {
    msg match {
      case cm:CamelMessage => {
        // message was successfully sent - ack it
        sender() ! Ack
      }
      case x:Any => println(x)
    }
  }
}