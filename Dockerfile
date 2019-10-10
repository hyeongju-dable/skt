FROM godatadriven/pyspark

ARG kafka_version=2.2.1
ARG scala_version=2.12

ENV CLASSPATH=/usr/local/openjdk-8/bin/
ENV KAFKA_VERSION=$kafka_version
ENV SCALA_VERSION=$scala_version
ENV KAFKA_HOME=/opt/kafka
    
RUN apt update
RUN apt -y install wget
RUN apt install -y postgresql postgresql-contrib
RUN cd /opt && \
    wget http://apache.mirror.cdnetworks.com/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    tar xvf kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    ln -s /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} ${KAFKA_HOME}

RUN mkdir /home/app
COPY . /home/app
WORKDIR /home/app
RUN pip install -r requirements.txt
ENTRYPOINT ["/bin/bash", "run.sh"]