package com.kozitski.converter.writer;

import com.kozitski.converter.domain.DestinationDTO;
import com.kozitski.converter.domain.TestDTO;
import com.kozitski.converter.io.DestinationCsvReader;
import com.kozitski.converter.io.TestCsvReader;
import com.kozitski.converter.schema.DestinationSchemaGenerator;
import com.kozitski.converter.schema.TestSchemaGenerator;
import org.apache.avro.Schema;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class DestinationWriter {

    private static final String AVRO_FILE_PATH = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/avro_data/destinations.avro";

    public static void main(String[] args) {

        Schema schema = new DestinationSchemaGenerator().generate();
        writeToAvro(schema);

        readFromAvroFile(schema);

    }

    private static void writeToAvro(Schema schema) {

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = null;

        try {
            Configuration conf = new Configuration();

            FileSystem fs = FileSystem.get(URI.create(AVRO_FILE_PATH), conf);
            OutputStream out = fs.create(new Path(AVRO_FILE_PATH));

            dataFileWriter = new DataFileWriter<>(datumWriter);

            dataFileWriter.create(schema, out);

            DestinationCsvReader destinationCsvReader = new DestinationCsvReader();
            while (destinationCsvReader.isHasMore()){
                List<DestinationDTO> testDTOS = destinationCsvReader.readPart();

                List<GenericRecord> tests = new LinkedList<>();
                testDTOS.forEach(e -> {
                    if(e != null){
                        GenericData.Record record = new GenericData.Record(schema);

                        record.put("srchDestinationId", e.getSrchDestinationId().orElse(null));
                        List<Optional<Double>> list = e.getD();
                        for (int i = 0; i < list.size(); i++) {
                            record.put("d" + (i + 1), list.get(i).orElse(null));
                        }

                        tests.add(record);
                    }
                });
                for (GenericRecord record: tests) {
                    dataFileWriter.append(record);
                    System.out.println("....record was appended");
                }
            }

        }
        catch (IOException e) {
            System.err.println("Exception during writing AVRO");
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

    // now it is only for testing
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
