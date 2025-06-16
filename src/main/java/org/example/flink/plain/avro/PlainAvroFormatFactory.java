/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example.flink.plain.avro;

import lombok.extern.log4j.Log4j2;
import org.apache.avro.Schema;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.avro.AvroRowDataDeserializationSchema;
import org.apache.flink.formats.avro.AvroRowDataSerializationSchema;
import org.apache.flink.formats.avro.AvroToRowDataConverters;
import org.apache.flink.formats.avro.RowDataToAvroConverters;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.example.flink.plain.avro.PlainAvroFormatOptions.SCHEMA;


/**
 * Table format factory for providing configured instances of Avro Schema to RowData {@link
 * SerializationSchema} and {@link DeserializationSchema}.
 */
@Internal
public class PlainAvroFormatFactory
        implements DeserializationFormatFactory, SerializationFormatFactory {

    public static final String IDENTIFIER = "plain-avro";

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        
        return new DecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                    DynamicTableSource.Context context, DataType producedDataType) {
                final RowType rowType = (RowType) producedDataType.getLogicalType();
                
                // This is being used to get the plain-avro.schema field from the statement
                Schema schema = formatOptions.getOptional(SCHEMA)
                        .map(schemaAsString -> new Schema.Parser().parse(schemaAsString))
                        .orElseGet(() -> AvroSchemaConverter.convertToSchema(rowType));

                final TypeInformation<RowData> rowDataTypeInfo =
                        context.createTypeInformation(producedDataType);
                return new AvroRowDataDeserializationSchema(
                        // Main change will be to use a custom deserializer as the nestedSchema
                        new RowAvroFlinkDeserializer(schema),
                        AvroToRowDataConverters.createRowConverter(rowType),
                        rowDataTypeInfo);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);

        return new EncodingFormat<SerializationSchema<RowData>>() {
            @Override
            public SerializationSchema<RowData> createRuntimeEncoder(
                    DynamicTableSink.Context context, DataType consumedDataType) {
                
                final RowType rowType = (RowType) consumedDataType.getLogicalType();
                Schema schema = formatOptions.getOptional(SCHEMA)
                        .map(schemaAsString -> new Schema.Parser().parse(schemaAsString))
                        .orElseGet(() -> AvroSchemaConverter.convertToSchema(rowType));

                return new AvroRowDataSerializationSchema(
                        rowType,
                        new RowAvroFlinkSerializer(schema),
                        RowDataToAvroConverters.createConverter(consumedDataType.getLogicalType())
                );
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SCHEMA);
        return options;
    }
    
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }
}