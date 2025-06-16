CREATE TABLE Student_table
(
    `name` STRING NOT NULL,
    `id` STRING NOT NULL,
    `age` INT
) WITH (
      'connector' = 'kafka',
      'topic' = 'input.topic',
      'properties.bootstrap.servers' = 'localhost:9092',
      'properties.group.id' = 'flink',
      'properties.auto.offset.reset' = 'latest',
      'value.format' = 'plain-avro',
      'value.plain-avro.schema' = '"{\"type\":\"record\",\"name\":\"Student\",\"namespace\":\"org.example\",\"fields\":[{\"name\":\"name\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"id\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"age\",\"type\":[\"null\",\"int\"]}]}"'
      )