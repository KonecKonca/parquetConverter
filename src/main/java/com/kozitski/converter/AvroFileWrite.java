package com.kozitski.converter;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

public class AvroFileWrite {

//    public static final String AVRO_FILE_PATH = "/home/maria_dev/person.avro";
    public static final String AVRO_FILE_PATH = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/test/testTest.avro";

    public static void main(String[] args) {

        Schema schema = parseSchema();
        writeToAvro(schema);
        readFromAvroFile(schema);

    }

    private static Schema parseSchema() {

        return SchemaBuilder.record("X")
                .fields()

                    .name("date_time")
                    .type("string")
                    .noDefault()

                    .name("site_name")
                    .type("int")
                    .noDefault()

                    .name("posa_continent")
                    .type("int")
                    .noDefault()

                    .name("user_location_country")
                    .type("int")
                    .noDefault()

                    .name("user_location_region")
                    .type("int")
                    .noDefault()

                    .name("user_location_city")
                    .type("int")
                    .noDefault()

                    .name("orig_destination_distance")
                    .type("double")
                    .noDefault()

                    .name("user_id")
                    .type("int")
                    .noDefault()

                    .name("is_mobile")
                    .type("int")
                    .noDefault()

                    .name("is_package")
                    .type("int")
                    .noDefault()

                    .name("channel")
                    .type("int")
                    .noDefault()

                    .name("srch_ci")
                    .type("string")
                    .noDefault()

                    .name("srch_co")
                    .type("string")
                    .noDefault()

                    .name("srch_adults_cnt")
                    .type("int")
                    .noDefault()

                    .name("srch_children_cnt")
                    .type("int")
                    .noDefault()

                    .name("srch_rm_cnt")
                    .type("int")
                    .noDefault()

                    .name("srch_destination_id")
                    .type("int")
                    .noDefault()

                    .name("srch_destination_type_id")
                    .type("int")
                    .noDefault()

                    .name("hotel_continent")
                    .type("int")
                    .noDefault()

                    .name("hotel_country")
                    .type("int")
                    .noDefault()

                    .name("hotel_market")
                    .type("int")
                    .noDefault()

                    .name("is_booking")
                    .type("int")
                    .noDefault()

                    .name("cnt")
                    .type("long")
                    .noDefault()

                    .name("hotel_cluster")
                    .type("int")
                    .noDefault()

                .endRecord();

    }

    private static void writeToAvro(Schema schema) {

        GenericRecord person1 = new GenericData.Record(schema);
        person1.put("date_time", "1111");
        person1.put("site_name", 343);
        person1.put("posa_continent", 4);
        person1.put("user_location_country", 343);
        person1.put("user_location_region", 354);
        person1.put("user_location_city", 5465);
        person1.put("orig_destination_distance", 35.5);
        person1.put("user_id", 5465);
        person1.put("is_mobile", 5465);
        person1.put("is_package", 5465);
        person1.put("channel", 3434);
        person1.put("srch_ci", "1, Richmond Drive");
        person1.put("srch_co", "1, Richmond Drive");
        person1.put("srch_adults_cnt", 35);
        person1.put("srch_children_cnt", 335);
        person1.put("srch_rm_cnt", 5);
        person1.put("srch_destination_id", 3);
        person1.put("srch_destination_type_id", 3);
        person1.put("hotel_continent", 2);
        person1.put("hotel_country", 2);
        person1.put("hotel_market", 2);
        person1.put("is_booking", 3);
        person1.put("cnt", 3L);
        person1.put("hotel_cluster", 3);

//        GenericRecord person2 = new GenericData.Record(schema);
//        person2.put("id", 2);
//        person2.put("Name", "Jill");
//        person2.put("Address", "2, Richmond Drive");

        DatumWriter<GenericRecord> datumWriter = new
                GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = null;

        try {
            Configuration conf = new Configuration();

            FileSystem fs = FileSystem.get(URI.create(AVRO_FILE_PATH), conf);
            OutputStream out = fs.create(new Path(AVRO_FILE_PATH));

            dataFileWriter = new DataFileWriter<>(datumWriter);

            dataFileWriter.create(schema, out);

            dataFileWriter.append(person1);
//            dataFileWriter.append(person2);
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        finally {
            if(dataFileWriter != null) {
                try {
                    dataFileWriter.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

        }
    }

    private static void readFromAvroFile(Schema schema) {

        Configuration conf = new Configuration();
        DataFileReader<GenericRecord> dataFileReader = null;
        try {
            // change the IP
            FsInput in = new FsInput(new Path(AVRO_FILE_PATH), conf);
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

            dataFileReader = new DataFileReader<>(in, datumReader);
            GenericRecord person = null;

            while (dataFileReader.hasNext()) {
                person = dataFileReader.next(person);
                System.out.println("\t" + person);
            }

        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally {
            if (dataFileReader != null) {
                try {
                    dataFileReader.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }

    }

}
