package Cluster

import java.net.URLEncoder
import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, Props, SupervisorStrategy}
import scala.collection.mutable

object JobReceptionist {
  def props = Props(new JobReceptionist)
  def name = "receptionist"
  case class JobResult(name: String, result: mutable.Queue[String])
  sealed trait Response
  case class JobRequest(name: String, toProcess: mutable.Queue[Int])
  case class JobSuccess(name: String, result: mutable.Queue[String]) extends Response
  case class JobFailure(name: String) extends Response
  case class Job(name: String, toProcess: mutable.Queue[Int], respondTo: ActorRef, jobMaster : ActorRef)
}

class JobReceptionist extends Actor
                          with ActorLogging
                          with CreateMaster{
  import JobReceptionist._
  import JobMaster.StartJob
  import context._

  override def supervisorStrategy: SupervisorStrategy = SupervisorStrategy.stoppingStrategy

  var jobs = Set[Job]()
  var retries = Map[String, Int]()
  val maxRetries = 3

  override def receive: Receive = {
    case JobRequest(name, toProcess) =>
      log.info(s"Received job $name")
      val masterName = "master-"+URLEncoder.encode(name, "UTF8")
      val jobMaster = createMaster(masterName)

      jobs = jobs + Job(name, toProcess, sender, jobMaster)
      jobMaster ! StartJob(name, toProcess)
      watch(jobMaster)

    case JobResult(jobName, result) =>
      log.info(s"result:${result}")
      jobs.find(_.name == jobName).foreach { job =>
        job.respondTo ! JobSuccess(jobName, result)
        stop(job.jobMaster)
        jobs = jobs - job
      }
  }
}

trait CreateMaster {
  def context: ActorContext
  def createMaster(name: String) = context.actorOf(JobMaster.props, name)
}