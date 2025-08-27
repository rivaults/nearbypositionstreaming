mvn io.confluent:kafka-schema-registry-maven-plugin:register
mvn exec:java -Dexec.mainClass=org.beam.Main -Dexec.args="--runner=FlinkRunner \
      --flinkMaster=jobmanager:8081 \
      --filesToStage=target/nearby-stream-1.0-shaded.jar"