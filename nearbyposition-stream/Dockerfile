FROM maven:3.9.11-amazoncorretto-11-alpine@sha256:bf9b5a9f22db0bc996fae11eb16c82857771f2ad0aec8e09eac6f892532b4fce
COPY pom.xml .
RUN mvn dependency:go-offline

COPY check-status.sh .
RUN apk update
RUN apk add curl
RUN apk add jq
RUN apk add --no-cache libc6-compat

RUN mvn -Dhttp.keepAlive=false -Dmaven.wagon.http.pool=false -Dmaven.wagon.http.retryHandler.count=5 dependency:resolve-plugins

COPY src src
RUN mvn -Dhttp.keepAlive=false -Dmaven.wagon.http.pool=false -Dmaven.wagon.http.retryHandler.count=5 -C clean package

COPY command.sh .
CMD bash command.sh