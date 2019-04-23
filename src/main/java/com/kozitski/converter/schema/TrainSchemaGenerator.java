package com.kozitski.converter.schema;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class TrainSchemaGenerator implements SchemaGenerator{

    @Override
    public Schema generate() {
        return SchemaBuilder
            .record("Train")
                .fields()
                .optionalString("dateTime")
                .optionalInt("siteName")
                .optionalInt("posaContinent")
                .optionalInt("userLocationCountry")
                .optionalInt("userLocationRegion")
                .optionalInt("userLocationCity")
                .optionalDouble("origDestinationDistance")
                .optionalInt("userId")
                .optionalInt("isMobile")
                .optionalInt("isPackage")
                .optionalInt("channel")
                .optionalString("srchCi")
                .optionalString("srchCo")
                .optionalInt("srchAdultsCnt")
                .optionalInt("srchChildrenCnt")
                .optionalString("srchRmCnt")
                .optionalInt("srchDestinationId")
                .optionalInt("srchDestinationTypeId")

                .optionalInt("isBooking")
                .optionalLong("cnt")

                .optionalInt("hotelContinent")
                .optionalInt("hotelCountry")
                .optionalInt("hotelMarket")

                .optionalInt("hotelCluster")

            .endRecord();
    }

}
