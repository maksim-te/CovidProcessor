FROM docker.io/library/spark:3.5.6

COPY target/scala-2.12/CovidProcessor-assembly-1.0.0.jar /opt/custom_jars/CovidProcessor-assembly-1.0.0.jar
