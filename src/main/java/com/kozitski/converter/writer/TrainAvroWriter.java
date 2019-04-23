package com.kozitski.converter.writer;

import com.kozitski.converter.domain.TrainDTO;
import com.kozitski.converter.io.TrainCsvReader;
import com.kozitski.converter.schema.TrainSchemaGenerator;
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

public class TrainAvroWriter {

    private static final String AVRO_FILE_PATH = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/avro_data/train.avro";

    public static void main(String[] args) {

        Schema schema = new TrainSchemaGenerator().generate();
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

            TrainCsvReader trainCsvReader = new TrainCsvReader();
            while (trainCsvReader.isHasMore()){
                List<TrainDTO> trainDTOS = trainCsvReader.readPart();

                List<GenericRecord> tests = new LinkedList<>();
                trainDTOS.forEach(e -> {
                    if(e != null){
                        GenericData.Record record = new GenericData.Record(schema);

                        record.put("dateTime", e.getDateTime().orElse(null));
                        record.put("siteName", e.getSiteName().orElse(null));
                        record.put("posaContinent", e.getPosaContinent().orElse(null));
                        record.put("userLocationCountry", e.getUserLocationCountry().orElse(null));
                        record.put("userLocationRegion", e.getUserLocationRegion().orElse(null));
                        record.put("userLocationCity", e.getUserLocationCity().orElse(null));
                        record.put("origDestinationDistance", e.getOrigDestinationDistance().orElse(null));
                        record.put("userId", e.getUserId().orElse(null));
                        record.put("isMobile", e.getIsMobile().orElse(null));
                        record.put("isPackage", e.getIsPackage().orElse(null));
                        record.put("channel", e.getChannel().orElse(null));
                        record.put("srchCi", e.getSrchCi().orElse(null));
                        record.put("srchCo", e.getSrchCo().orElse(null));
                        record.put("srchAdultsCnt", e.getSrchAdultsCnt().orElse(null));
                        record.put("srchChildrenCnt", e.getSrchChildrenCnt().orElse(null));
                        record.put("srchRmCnt", e.getSrchRmCnt().orElse(null));
                        record.put("srchDestinationId", e.getSrchDestinationId().orElse(null));
                        record.put("srchDestinationTypeId", e.getSrchDestinationTypeId().orElse(null));

                        record.put("isBooking", e.getIsBooking().orElse(null));
                        record.put("cnt", e.getCnt().orElse(null));

                        record.put("hotelContinent", e.getHotelContinent().orElse(null));
                        record.put("hotelCountry", e.getHotelCountry().orElse(null));
                        record.put("hotelMarket", e.getHotelMarket().orElse(null));

                        record.put("hotelCluster", e.getHotelCluster().orElse(null));

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
