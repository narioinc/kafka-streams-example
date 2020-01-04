export CLASSPATH=target/classes
mvn package
mvn exec:java -Dexec.mainClass=com.narioinc.kafkastreams.Pipe
