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
    public static final String AVRO_FILE_PATH = "/user/maria_dev/test/person.avro";

    public static void main(String[] args) {

        Schema schema = parseSchema();
        writeToAvro(schema);
        readFromAvroFile(schema);

    }

    private static Schema parseSchema() {

        return SchemaBuilder.record("X")
                .fields()

                    .name("id")
                    .type("int")
                    .noDefault()

                    .name("Name")
                    .type("string")
                    .noDefault()

                    .name("Address")
                    .type("string")
                    .noDefault()

                .endRecord();

    }

    private static void writeToAvro(Schema schema) {

        GenericRecord person1 = new GenericData.Record(schema);
        person1.put("id", 1);
        person1.put("Name", "Jack");
        person1.put("Address", "1, Richmond Drive");

        GenericRecord person2 = new GenericData.Record(schema);
        person2.put("id", 2);
        person2.put("Name", "Jill");
        person2.put("Address", "2, Richmond Drive");

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
            dataFileWriter.append(person2);
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
