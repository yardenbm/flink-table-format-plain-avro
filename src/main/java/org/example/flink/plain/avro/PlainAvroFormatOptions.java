package org.example.flink.plain.avro;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class PlainAvroFormatOptions {

    public static final ConfigOption<String> SCHEMA = ConfigOptions
            .key("schema")
            .stringType()
            .noDefaultValue()
            .withDescription("when using plain-avro format, this should contain the corresponding Avro schema " +
                    "That is being used in the current context. If this field is not set, schema will be infered from " +
                    "the table, same as in avro format.");
}