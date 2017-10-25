package Cluster

import akka.actor._
import akka.cluster.routing._
import akka.routing._
import scala.collection.mutable.Queue
import scala.concurrent.duration._

object JobMaster{
  def props = Props(new JobMaster)
  case class StartJob(name: String, jobs: Queue[Int])
  case class Enlist(worker: ActorRef)
  case object NextTask
  case class TaskResult(requested: Int, result: Int)
  case object Start
  case class DisplayResults(result : Int)
}

class JobMaster extends Actor
                    with ActorLogging
                    with CreateWorkerRouter{
  import JobReceptionist.JobResult
  import JobMaster._
  import JobWorker._
  import context._

  var toProcess = Queue[Int]()
  var resultSet = Queue[String]()

  var workGiven = 0
  var workReceived = 0
  var workers = Set[ActorRef]()
  val router = createWorkerRouter

  def receive = idle

  def idle : Receive = {
    case StartJob(jobName, jobs) =>
      toProcess = jobs
      val cancellable = context.system.scheduler.schedule(0 millis, 1000 millis, router, Work(jobName, self))
      context.setReceiveTimeout(60 seconds)

      become(working(jobName, sender, cancellable))
  }

  def working(jobName: String,
              receptionist: ActorRef,
              cancellable: Cancellable): Receive = {
    case Enlist(worker) =>
      watch(worker)
      workers  = workers + worker

    case NextTask =>
      if(toProcess.nonEmpty){
        sender() ! Task(toProcess.dequeue(), self)
        workGiven = workGiven + 1
      }

    case TaskResult(requested, result) =>
      log.info(s"DONE $requested and result is $result.")
      workReceived = workReceived + 1
      resultSet.enqueue(s"$requested = $result")
      if(workGiven == workReceived) {
        cancellable.cancel()
        become(finishing(jobName, receptionist, workers))
        setReceiveTimeout(Duration.Undefined)
        self ! DisplayResults
      }

    case ReceiveTimeout =>
      if(workers.isEmpty) {
        log.info(s"No workers responded in time. Cancelling job $jobName.")
        stop(self)
      } else setReceiveTimeout(Duration.Undefined)

    case Terminated(worker) =>
      log.info(s"Worker $worker got terminated. Cancelling job $jobName.")
      stop(self)
  }

  def finishing(jobName: String,
                receptionist: ActorRef,
                workers: Set[ActorRef]): Receive = {
    case Terminated(worker) =>
      log.info(s"Job $jobName is finishing. Worker ${worker.path.name} is stopped.")

    case DisplayResults =>
      workers.foreach(stop(_))
      receptionist ! JobResult(jobName, resultSet)
  }
}

trait CreateWorkerRouter { this: Actor =>
  def createWorkerRouter: ActorRef = {
    context.actorOf(
      ClusterRouterPool(BroadcastPool(10), ClusterRouterPoolSettings(
        totalInstances = 100, maxInstancesPerNode = 20,
        allowLocalRoutees = false, useRoles = Set("worker"))).props(Props[JobWorker]),
      name = "worker-router")
  }
}