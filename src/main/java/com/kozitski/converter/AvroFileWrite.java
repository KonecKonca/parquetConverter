package com.kozitski.converter;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.Pair;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class AvroFileWrite {

    public static void main(String[] args) {

        Schema schema = parseSchema();
System.out.println("222222222222222222");

        writetoAvro(schema);

    }

    // parsing the schema
    private static Schema parseSchema() {
//        Schema.Parser parser = new Schema.Parser();
//        Schema schema = null;
//        try {
//            // Path to schema file
//System.out.println("!!!!!!!!!!!!!!!!!!!!!!!"); // My favourite NPE ))
//            schema = parser.parse(ClassLoader.getSystemResourceAsStream(
//                    "resources/Person.avsc"));
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return schema;

System.out.println("111111111111111111111111");

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

    private static void writetoAvro(Schema schema) {
        GenericRecord person1 = new GenericData.Record(schema);
        person1.put("id", 1);
        person1.put("Name", "Jack");
        person1.put("Address", "1, Richmond Drive");

        GenericRecord person2 = new GenericData.Record(schema);
        person2.put("id", 2);
        person2.put("Name", "Jill");
        person2.put("Address", "2, Richmond Drive");

        DatumWriter<GenericRecord> datumWriter = new
                GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = null;
        try {
            //out file path in HDFS
            Configuration conf = new Configuration();
            // change the IP
            FileSystem fs = FileSystem.get(URI.create("/home/maria_dev/person.avro"), conf);
            OutputStream out = fs.create(new Path("/home/maria_dev/person.avro"));

            dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
            // for compression
            //dataFileWriter.setCodec(CodecFactory.snappyCodec());
            dataFileWriter.create(schema, out);

            dataFileWriter.append(person1);
            dataFileWriter.append(person2);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally {
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


}
