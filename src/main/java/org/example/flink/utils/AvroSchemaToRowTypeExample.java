package org.example.flink.utils;

import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.types.logical.RowType;
import org.example.Student;

import java.io.IOException;

public class AvroSchemaToRowTypeExample {
    public static void main(String[] args) throws IOException {
        // Load Avro schema from a generated class
        String schemaStr = Student.SCHEMA$.toString();
        
        // Convert to Flink RowType
        RowType rowType = (RowType) AvroSchemaConverter.convertToDataType(schemaStr).getLogicalType();

        System.out.println(rowType); // Output the corresponding structure
    }
}
