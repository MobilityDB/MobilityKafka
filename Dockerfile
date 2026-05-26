FROM debian:bookworm-slim

RUN apt-get update \
  && apt-get install -y git curl gnupg build-essential cmake \
     postgresql-server-dev-15 libproj-dev libjson-c-dev libgsl-dev libgeos-dev postgis \
  && export GNUPGHOME="$(mktemp -d)" \
  && curl -fL https://apt.corretto.aws/corretto.key | gpg --batch --import \
  && gpg --batch --export '6DC3636DAE534049C8B94623A122542AB04F24E3' > /usr/share/keyrings/corretto.gpg \
  && rm -r "$GNUPGHOME" \
  && unset GNUPGHOME \
  && echo "deb [signed-by=/usr/share/keyrings/corretto.gpg] https://apt.corretto.aws stable main" > /etc/apt/sources.list.d/corretto.list \
  && apt-get update \
  && apt-get install -y java-21-amazon-corretto-jdk

RUN git clone --depth 1 https://github.com/MobilityDB/MobilityDB.git -b stable-1.3 /usr/local/src/MobilityDB
RUN mkdir -p /usr/local/src/MobilityDB/build
RUN cd /usr/local/src/MobilityDB/build && \
    cmake -DMEOS=ON .. && \
    make -j$(nproc) && \
    make install && \
    ldconfig

ENV MAVEN_HOME=/usr/share/maven
ENV MAVEN_CONFIG="/root/.m2"
COPY --from=maven:3.9.6-eclipse-temurin-11 ${MAVEN_HOME} ${MAVEN_HOME}
COPY --from=maven:3.9.6-eclipse-temurin-11 /usr/local/bin/mvn-entrypoint.sh /usr/local/bin/mvn-entrypoint.sh
COPY --from=maven:3.9.6-eclipse-temurin-11 /usr/share/maven/ref/settings-docker.xml /usr/share/maven/ref/settings-docker.xml
RUN ln -s ${MAVEN_HOME}/bin/mvn /usr/bin/mvn

COPY jar/JMEOS.jar /tmp/JMEOS.jar
RUN mvn install:install-file \
    -Dfile=/tmp/JMEOS.jar \
    -DgroupId=org.jmeos \
    -DartifactId=jmeos \
    -Dversion=1.0-SNAPSHOT \
    -Dpackaging=jar

WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -q
COPY src ./src
RUN mvn package -DskipTests

ENV LD_LIBRARY_PATH=/usr/local/lib

CMD ["java", "--add-opens", "java.base/java.lang=ALL-UNNAMED", \
     "-Djava.library.path=/usr/local/lib", \
     "-cp", "target/MobilityKafka-1.0-SNAPSHOT-jar-with-dependencies.jar:/tmp/JMEOS.jar", \
     "Queries.Query7_Main"]