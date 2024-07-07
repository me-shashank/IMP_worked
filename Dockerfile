
FROM eu.gcr.io/BASE_IMAGE_GCP_PROJECT/nifi-minifi:0.6.0

USER root

ENV MINIFI_HOME /opt/minifi/minifi-0.5.0
ENV MOUNT_PATH /mnt

RUN apt-get update
RUN apt-get install telnet

ADD config.yml $MINIFI_HOME/conf/config.yml
ADD minifi-scripts/minifi.sh $MINIFI_HOME/bin/minifi.sh

RUN rm -rf $MINIFI_HOME/lib/nifi-standard*.nar
RUN rm -rf $MINIFI_HOME/lib/nifi-standard-services-api-nar-*.nar
RUN rm -rf $MINIFI_HOME/lib/nifi-record-serialization-services-nar*.nar

#Add nifi nars to minifi lib
ADD minifi-processor-nars/* $MINIFI_HOME/lib/

#make jdbcjars directory for custom libraries
RUN mkdir -p $MINIFI_HOME/jdbcjars
ENV JDBC_JARS $MINIFI_HOME/jdbcjars
#copy the artifacts from artifacts directory

ADD nifi-jdbc-connectors/cloud-sql/* $MINIFI_HOME/jdbcjars/
ADD sims-encryption-key/* $MINIFI_HOME/jdbcjars/

ADD nifi-jdbc-connectors/oracle/* $MINIFI_HOME/jdbcjars/

ADD minifi-application-conf/logback.xml $MINIFI_HOME/conf/logback.xml
ADD minifi-application-conf/bootstrap.conf $MINIFI_HOME/conf/bootstrap.conf
RUN chmod 770 $MINIFI_HOME/bin/minifi.sh
RUN chmod +x /opt/minifi/scripts/start.sh
RUN chown -R minifi:minifi $MINIFI_HOME

USER minifi

