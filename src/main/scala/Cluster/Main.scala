package Cluster

import com.typesafe.config.ConfigFactory
import akka.actor.{ActorSystem, Props}
import akka.cluster.Cluster
import JobReceptionist.JobRequest
import scala.collection.mutable

object Main extends App{
    val conf = ConfigFactory.load
    val system = ActorSystem("square", conf)

    println(s"Start node with roles: ${Cluster(system).selfRoles}")

    if(system.settings.config.getStringList("akka.cluster.roles").contains("master")) {
      Cluster(system).registerOnMemberUp {

        val receptionist = system.actorOf(JobReceptionist.props, "receptionist")
        println("Master node is ready.")

         receptionist ! JobRequest(s"Test Job", mutable.Queue(5,12,123,555))

         system.actorOf(Props(new ClusterDomainEventListener), "cluster-listener")
      }
    }
}
