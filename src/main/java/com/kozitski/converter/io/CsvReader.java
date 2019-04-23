package com.kozitski.converter.io;

import com.kozitski.converter.domain.TestDTO;
import lombok.Getter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class CsvReader {
    private static final int PARTS_SIZE = 100_000;
    private int counter = 0;
    @Getter private boolean hasMore = true;
    private BufferedReader br;

    private static final String READING_FILE_PATH = "/user/maria_dev/data/test.csv";
//    private static final String READING_FILE_PATH = "C:\\Users\\Andrei_Kazitski\\Desktop\\dataData\\unziped\\test.csv";

    public static void main(String[] args) {
        new CsvReader().readPart();
    }

    public CsvReader(){
        Path pt = new Path(READING_FILE_PATH);
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            br = new BufferedReader(new InputStreamReader(fs.open(pt)));
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    public List<TestDTO> readPart(){
        List<TestDTO> result = new LinkedList<>();

        try{
            String line;
            line = br.readLine();

            int writeCounter = 0;
            while (line != null && counter++ < PARTS_SIZE ){
                line = br.readLine();
                result.add(parseLine(line));
System.out.println(writeCounter++);
            }

            if(line == null){
                hasMore = false;
            }

            counter = 0;
        }
        catch(Exception e){
            System.err.println("poblems during file reading");
            e.printStackTrace();
        }

        return result;
    }

    private TestDTO parseLine(String line) {
        TestDTO result = null;

        if(line != null && !line.isEmpty()){
            String[] fields = line.split(",");

            result = TestDTO.builder()
                    .id(Optional.of(fields[0]))
                    .date_time(Optional.of(fields[1]))
                    .site_name(intWithNulParse(fields[2]))
                    .posa_continent(intWithNulParse(fields[3]))
                    .user_location_country(intWithNulParse(fields[4]))
                    .user_location_region(intWithNulParse(fields[5]))
                    .user_location_city(intWithNulParse(fields[6]))
                    .orig_destination_distance(doubleWithNulParse(fields[7]))
                    .user_id(intWithNulParse(fields[8]))
                    .is_mobile(intWithNulParse(fields[9]))
                    .is_package(intWithNulParse(fields[10]))
                    .channel(intWithNulParse(fields[11]))
                    .srch_ci(Optional.of(fields[12]))
                    .srch_co(Optional.of(fields[13]))
                    .srch_adults_cnt(intWithNulParse(fields[14]))
                    .srch_children_cnt(intWithNulParse(fields[15]))
                    .srch_rm_cnt(Optional.of(fields[16]))
                    .srch_destination_id(intWithNulParse(fields[17]))
                    .srch_destination_type_id(intWithNulParse(fields[18]))
                    .hotel_continent(intWithNulParse(fields[19]))
                    .hotel_country(intWithNulParse(fields[20]))
                    .hotel_market(intWithNulParse(fields[21]))
                    .build();
        }

        return result;
    }

    private Optional<Integer> intWithNulParse(String str){
        Optional<Integer> result = Optional.empty();

        if (str != null && !str.isEmpty()){
            result = Optional.of(Integer.parseInt(str));
        }

        return result;
    }
    private Optional<Double> doubleWithNulParse(String str){
        Optional<Double> result = Optional.empty();

        if (str != null && !str.isEmpty()){
            result = Optional.of(Double.parseDouble(str));
        }

        return result;
    }
    private Optional<Long> longWithNulParse(String str){
        Optional<Long> result = Optional.empty();

        if (str != null && !str.isEmpty()){
            result = Optional.of(Long.parseLong(str));
        }

        return result;
    }

}
