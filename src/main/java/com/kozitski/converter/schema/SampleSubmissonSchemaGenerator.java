package com.kozitski.converter.schema;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class SampleSubmissonSchemaGenerator implements SchemaGenerator{

    @Override
    public Schema generate() {
        return SchemaBuilder
            .record("Test")
            .fields()
                .optionalInt("id")
                .optionalString("hotelCluster")
            .endRecord();
    }


}
