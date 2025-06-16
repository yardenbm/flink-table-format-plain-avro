package org.example.flink.plain.avro;

import lombok.extern.log4j.Log4j2;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.util.Arrays;

public class RowAvroFlinkDeserializer implements DeserializationSchema<GenericRecord> {

    private final Schema schema;

    public RowAvroFlinkDeserializer(Schema schema) {
        this.schema = schema;
    }

    @Override
    public GenericRecord deserialize(byte[] data) throws IOException {
        try {
            if (data == null || data.length == 0) {
                throw new SerializationException("Empty or null Avro data received.");
            }
            
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
            return datumReader.read(null,
                    DecoderFactory.get().binaryDecoder(data, null));

        } catch (Exception e) {
            throw new SerializationException("Can't deserialize data: " + Arrays.toString(data), e);
        }
    }

    @Override
    public boolean isEndOfStream(GenericRecord record) {
        return false;
    }

    @Override
    public TypeInformation<GenericRecord> getProducedType() {
        return TypeExtractor.getForClass(GenericRecord.class);
    }
}
