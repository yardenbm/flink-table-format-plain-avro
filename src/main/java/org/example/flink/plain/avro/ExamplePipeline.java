package org.example.flink.plain.avro;

import org.apache.avro.Schema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.Student;

import java.util.Objects;

public class ExamplePipeline {
    public static void main(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Schema avroSchema = Student.SCHEMA$;
        // Register source table form kafka source
        tableEnv.createTemporaryTable(avroSchema.getName() + "_table",
                TableDescriptor.forConnector("kafka")
                        .schema(buildFlinkSchema(avroSchema))
                        .option("topic", params.get("input.topic"))
                        .option("properties.bootstrap.servers", params.get("kafka.bootstrap.server"))
                        .option("properties.auto.offset.reset", "latest")
                        .option("properties.group.id", params.get("consumer.group"))
                        .option("value.format", "plain-avro")
                        .option("value.plain-avro.schema", avroSchema.toString())
                        .build());
        
        // For sake of example - print whatever is being consumed from kafka topic
        tableEnv.executeSql("SELECT * FROM " + avroSchema.getName() + "_table");
    }
    
    private static org.apache.flink.table.api.Schema buildFlinkSchema(Schema avroSchema) {
        org.apache.flink.table.api.Schema.Builder schemaBuilder = org.apache.flink.table.api.Schema.newBuilder();
        schemaBuilder.fromRowDataType(AvroSchemaConverter.convertToDataType(avroSchema.toString()));

        return schemaBuilder.build();
    }

    
}