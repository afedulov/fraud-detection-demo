FROM flink:1.9.1

COPY build/libs/dynamic-fraud-detection-demo.jar lib/job.jar
COPY docker-entrypoint.sh /

USER flink
EXPOSE 8081 6123
ENTRYPOINT ["/docker-entrypoint.sh"]
