package com.kozitski.converter.io;

import com.kozitski.converter.domain.TestDTO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class CsvReader {
    private static final String READING_FILE_PATH = "/user/maria_dev/data/test.csv";

    public static void main(String[] args) {
        List<TestDTO> result = new ArrayList<>();

        try{
            Path pt = new Path(READING_FILE_PATH);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            line = br.readLine();

            while (line != null){
                System.out.println("_____" + line + "_____");
                line = br.readLine();
            }

        }
        catch(Exception e){ /* NOP */ }

//        return result;
    }

    public List<TestDTO> readAll(){
        List<TestDTO> result = new LinkedList<>();

        try{
            Path pt = new Path(READING_FILE_PATH);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            line = br.readLine();

            while (line != null){
                line = br.readLine();
                result.add(parseLine(line));
            }

        }
        catch(Exception e){ /* NOP */ }

        return result;
    }

    private TestDTO parseLine(String line) {
        String[] fields = line.split(",");

        return TestDTO.builder()
                        .id(Optional.of(fields[0]))
                        .date_time(Optional.of(fields[1]))
                        .site_name(Optional.of(intWithNulParse(fields[2])))
                        .posa_continent(Optional.of(intWithNulParse(fields[3])))
                        .user_location_country(Optional.of(intWithNulParse(fields[4])))
                        .user_location_region(Optional.of(intWithNulParse(fields[5])))
                        .user_location_city(Optional.of(intWithNulParse(fields[6])))
                        .orig_destination_distance(Optional.of(doubleWithNulParse(fields[7])))
                        .user_id(Optional.of(intWithNulParse(fields[8])))
                        .is_mobile(Optional.of(intWithNulParse(fields[9])))
                        .is_package(Optional.of(intWithNulParse(fields[10])))
                        .channel(Optional.of(intWithNulParse(fields[11])))
                        .srch_ci(Optional.of(fields[12]))
                        .srch_co(Optional.of(fields[13]))
                        .srch_adults_cnt(Optional.of(intWithNulParse(fields[14])))
                        .srch_children_cnt(Optional.of(intWithNulParse(fields[15])))
                        .srch_rm_cnt(Optional.of(longWithNulParse(fields[16])))
                        .srch_destination_id(Optional.of(intWithNulParse(fields[17])))
                        .srch_destination_type_id(Optional.of(intWithNulParse(fields[18])))
                        .hotel_continent(Optional.of(intWithNulParse(fields[19])))
                        .hotel_country(Optional.of(intWithNulParse(fields[20])))
                        .hotel_market(Optional.of(intWithNulParse(fields[21])))
                .build();
    }

    private Integer intWithNulParse(String str){
        Integer result = null;

        if (str != null){
            result = Integer.parseInt(str);
        }

        return result;
    }
    private Double doubleWithNulParse(String str){
        Double result = null;

        if (str != null){
            result = Double.parseDouble(str);
        }

        return result;
    }
    private Long longWithNulParse(String str){
        Long result = null;

        if (str != null){
            result = Long.parseLong(str);
        }

        return result;
    }

}
