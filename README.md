# Akka clustering with logging and retry example

sbt clean compile 
sbt -DPORT=2551 -DROLE="seed"  run
sbt -DPORT=2552 -DROLE="master"  run
sbt -DPORT=2553 -DROLE="worker"  run
