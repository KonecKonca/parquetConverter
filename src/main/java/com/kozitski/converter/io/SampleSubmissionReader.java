package com.kozitski.converter.io;

import com.kozitski.converter.domain.SampleSubmitionDTO;
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

public class SampleSubmissionReader {

    private static final int PARTS_SIZE = 100_000;
    private int cursor;
    @Getter
    private boolean hasMore = true;
    private BufferedReader br;

    private static final String READING_FILE_PATH = "/user/maria_dev/data/sample_submission.csv";

    public SampleSubmissionReader(){
        Path pt = new Path(READING_FILE_PATH);
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            br = new BufferedReader(new InputStreamReader(fs.open(pt)));
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }


    public List<SampleSubmitionDTO> readPart(){
        List<SampleSubmitionDTO> result = new LinkedList<>();

        try{
            String line;
            line = br.readLine();

            int writeCounter = 0;
            while (line != null && cursor++ < PARTS_SIZE ){
                line = br.readLine();
                result.add(parseLine(line));
                System.out.println(writeCounter++);
            }

            if(line == null){
                hasMore = false;
            }

            cursor = 0;
        }
        catch(Exception e){
            System.err.println("poblems during file reading");
            e.printStackTrace();
        }

        return result;
    }



    private SampleSubmitionDTO parseLine(String line) {
        SampleSubmitionDTO result = null;

        if(line != null && !line.isEmpty()){
            String[] fields = line.split(",");

            result = new SampleSubmitionDTO();
            result.setId(intWithNulParse(fields[0]));
            result.setHotelCluster(stringWithNulParse(fields[1]));

        }

        return result;
    }
    private Optional<String> stringWithNulParse(String str){
        Optional<String> result = Optional.empty();

        if (str != null && !str.isEmpty()){
            result = Optional.of(str);
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
