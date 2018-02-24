package it.gov.daf.executioncontexts

import javax.inject.Inject

import akka.actor.ActorSystem
import com.google.inject.ImplementedBy
import play.api.libs.concurrent.CustomExecutionContext

import scala.concurrent.ExecutionContext

@ImplementedBy(classOf[WsClientExecutionContextImpl])
trait WsClientExecutionContext extends ExecutionContext

class WsClientExecutionContextImpl @Inject()(system: ActorSystem)
  extends CustomExecutionContext(system, "wsclient.executor") with WsClientExecutionContext
