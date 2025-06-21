# Flink Table API Custom Avro Format Example

This is a simple example project demonstrating how to create a custom format for Apache Flink Table API that explicitly defines the schema for Avro data. This example shows how to bypass Flink's automatic schema inference when using Kafka as a connector.

> **Note**: This is just a minimal example showing how I implemented custom serialization/deserialization in Flink Table API.

## Quick Start

### Prerequisites
- Apache Flink
- Apache Kafka
- Maven

### Usage Examples
Those examples are also attached in the src.
#### 1. Using DDL (SQL)
```sql
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
```

#### 2. Using Java API
```java
Schema avroSchema = Student.SCHEMA$;
tableEnv.createTemporaryTable(avroSchema.getName() + "_table",
        TableDescriptor.forConnector("kafka")
                .schema(buildFlinkSchema(avroSchema)) // See at ExamplePipeline.java
                .option("topic", params.get("input.topic"))
                .option("properties.bootstrap.servers", params.get("kafka.bootstrap.server"))
                .option("properties.auto.offset.reset", "latest")
                .option("properties.group.id", params.get("consumer.group"))
                .option("value.format", "plain-avro")
                .option("value.plain-avro.schema", avroSchema.toString())
                .build());
```

## Implementation Details

- A short example on how to create a custom format for Flink Table API
- usage of explicit schema definition instead of automatic inference using Table API
- Both DDL and programmatic table creation usage examples

**Important:**  
This example includes the file `src/main/resources/META-INF/services/org.apache.flink.table.factories.Factory`.  
This file must exist at exactly this location in your project. It is required by Flink's service discovery mechanism to register your custom format factory (`PlainAvroFormatFactory`). Without this file, Flink will not be able to find and load your custom serialization/deserialization logic.

The key configuration options are:
- `value.format`: Set to `plain-avro`
- `value.plain-avro.schema`: Avro schema definition as JSON string

