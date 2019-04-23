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

public class TestCsvReader {
    private static final int PARTS_SIZE = 100_000;
    private int cursor;
    @Getter private boolean hasMore = true;
    private BufferedReader br;

    private static final String READING_FILE_PATH = "/user/maria_dev/data/test.csv";
//    private static final String READING_FILE_PATH = "C:\\Users\\Andrei_Kazitski\\Desktop\\dataData\\unziped\\test.csv";

    public TestCsvReader(){
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



    private TestDTO parseLine(String line) {
        TestDTO result = null;

        if(line != null && !line.isEmpty()){
            String[] fields = line.split(",");

            result = TestDTO.builder()
                    .id(stringWithNulParse(fields[0]))
                    .dateTime(stringWithNulParse(fields[1]))
                    .siteName(intWithNulParse(fields[2]))
                    .posaContinent(intWithNulParse(fields[3]))
                    .userLocationCountry(intWithNulParse(fields[4]))
                    .userLocationRegion(intWithNulParse(fields[5]))
                    .userLocationCity(intWithNulParse(fields[6]))
                    .origDestinationDistance(doubleWithNulParse(fields[7]))
                    .userId(intWithNulParse(fields[8]))
                    .isMobile(intWithNulParse(fields[9]))
                    .isPackage(intWithNulParse(fields[10]))
                    .channel(intWithNulParse(fields[11]))
                    .srchCi(stringWithNulParse(fields[12]))
                    .srchCo(stringWithNulParse(fields[13]))
                    .srchAdultsCnt(intWithNulParse(fields[14]))
                    .srchChildrenCnt(intWithNulParse(fields[15]))
                    .srchRmCnt(stringWithNulParse(fields[16]))
                    .srchDestinationId(intWithNulParse(fields[17]))
                    .srchDestinationTypeId(intWithNulParse(fields[18]))
                    .hotelContinent(intWithNulParse(fields[19]))
                    .hotelCountry(intWithNulParse(fields[20]))
                    .hotelMarket(intWithNulParse(fields[21]))
                    .build();
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
