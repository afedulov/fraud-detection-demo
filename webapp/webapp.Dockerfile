FROM node:10 as ui-build
RUN mkdir /home/node/app
WORKDIR /home/node/app

COPY package.json package-lock.json .npmrc tsconfig.json ./
COPY scripts scripts
RUN npm ci --unsafe-perm
COPY public public
COPY src/app src/app
COPY src/index.tsx src
COPY src/react-app-env.d.ts src

RUN npm run build

# Start with a base image containing Java runtime
FROM openjdk:8-jdk-alpine as main
WORKDIR /home/maven/
COPY src src
COPY pom.xml ./
COPY .mvn .mvn
COPY mvnw ./

COPY --from=ui-build /home/node/app/build /home/maven/target/classes/static/
RUN ./mvnw install
RUN mv target/demo-fraud-webapp*.jar demo-fraud-webapp.jar

# Add a volume pointing to /tmp
VOLUME /tmp

# Make port 5656 available to the world outside this container
EXPOSE 5656

#ADD target/demo-backend-*.jar demo-backend.jar

# Run the jar file
ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-Dspring.profiles.active=dev","-jar","demo-fraud-webapp.jar"]