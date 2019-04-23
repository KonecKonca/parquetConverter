package com.kozitski.converter.schema;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class DestinationSchemaGenerator implements SchemaGenerator{

    @Override
    public Schema generate() {
        SchemaBuilder.FieldAssembler<Schema> schemaFieldAssembler = SchemaBuilder.record("Destination").fields().optionalInt("srchDestinationId");

        for (int i = 1; i <= 149; i++){
            schemaFieldAssembler.optionalDouble("d" + i);
        }

        return schemaFieldAssembler.endRecord();
    }


}
