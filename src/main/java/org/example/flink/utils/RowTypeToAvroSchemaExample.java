package org.example.flink.utils;

import org.apache.avro.Schema;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.types.logical.*;

import java.io.IOException;

public class RowTypeToAvroSchemaExample {
    public static void main(String[] args) throws IOException {
        RowType rowType = RowType.of(
                new LogicalType[] {
                        new VarCharType(false, VarCharType.MAX_LENGTH),
                        new VarCharType(false, VarCharType.MAX_LENGTH),
                        new IntType(),
                },
                new String[] {
                        "id", "name", "age"
                }
        );
        
        System.out.println(rowType);
        Schema originalSchema = AvroSchemaConverter.convertToSchema(rowType);

        // Print the corresponding generated schema
        System.out.println(originalSchema.toString(true));

    }
}
