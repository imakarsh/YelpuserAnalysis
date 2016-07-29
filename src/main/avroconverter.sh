#!/usr/bin/env bash
curl -iv http://central.maven.org/maven2/org/apache/avro/avro-tools/1.7.7/avro-tools-1.7.7.jar > avro-tools-1.7.7.jar
java -jar avro-tools-1.7.7.jar fromjson --codec snappy --schema-file resources\yelp_review.avsc resources\yelp_academic_dataset_review.json > resources\yelp_review.avro

