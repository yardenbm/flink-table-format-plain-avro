package org.example.flink.plain.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.kafka.common.errors.SerializationException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class RowAvroFlinkSerializer implements SerializationSchema<GenericRecord> {

    private final Schema schema;

    public RowAvroFlinkSerializer(Schema schema) {
        this.schema = schema;
    }

    @Override
    public byte[] serialize(GenericRecord record) {
        if (record == null) {
            throw new SerializationException("GenericRecord to serialize is null.");
        }

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
            Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            datumWriter.write(record, encoder);
            encoder.flush();

            return out.toByteArray();

        } catch (IOException e) {
            throw new SerializationException("Failed to serialize Avro record: " + record, e);
        }
    }
}
