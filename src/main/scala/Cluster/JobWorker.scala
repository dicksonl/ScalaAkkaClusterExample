package Cluster

import akka.actor.{Actor, ActorLogging, ActorRef, Props, ReceiveTimeout, Terminated}
import scala.concurrent.duration._

object JobWorker {
  def props = Props(new JobWorker)
  case class Work(jobName: String, master: ActorRef)
  case class Task(input: Int, master: ActorRef)
  case object WorkLoadDepleted
}

class JobWorker extends Actor with ActorLogging {
  import JobMaster._
  import JobWorker._
  import context._

  var processed = 0

  def receive = idle

  def idle: Receive = {
    case Work(jobName, master) =>
      become(enlisted(jobName, master))

      log.info(s"Enlisted, will start requesting work for job '${jobName}'.")
      master ! Enlist(self)
      master ! NextTask
      watch(master)

      setReceiveTimeout(30 seconds)
  }

  def enlisted(jobName: String, master: ActorRef): Receive = {
    case ReceiveTimeout =>
      master ! NextTask

    case Task(input, master) =>
      processed = processed + 1
      master ! TaskResult(input, input * input)
      master ! NextTask

    case WorkLoadDepleted =>
      log.info(s"Work load ${jobName} is depleted, retiring...")
      setReceiveTimeout(Duration.Undefined)
      become(retired(jobName))

    case Terminated(master) =>
      setReceiveTimeout(Duration.Undefined)
      log.error(s"Master terminated that ran Job ${jobName}, stopping self.")
      stop(self)
  }

  def retired(jobName: String): Receive = {
    case Terminated(master) =>
      log.error(s"Master terminated that ran Job ${jobName}, stopping self.")
      stop(self)
    case _ => log.error("I'm retired.")
  }
}