package com.kozitski.converter.io;

import com.kozitski.converter.domain.TrainDTO;
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

public class TrainCsvReader {
    private static final int PARTS_SIZE = 100_000;
    private int cursor;
    @Getter
    private boolean hasMore = true;
    private BufferedReader br;

    private static final String READING_FILE_PATH = "/user/maria_dev/data/train.csv";
//    private static final String READING_FILE_PATH = "C:\\Users\\Andrei_Kazitski\\Desktop\\dataData\\unziped\\test.csv";
    public TrainCsvReader(){
        Path pt = new Path(READING_FILE_PATH);
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            br = new BufferedReader(new InputStreamReader(fs.open(pt)));
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }


    public List<TrainDTO> readPart(){
        List<TrainDTO> result = new LinkedList<>();

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


    private TrainDTO parseLine(String line) {
        TrainDTO result = null;

        if(line != null && !line.isEmpty()){
            String[] fields = line.split(",");

            result = TrainDTO.builder()
                    .dateTime(stringWithNulParse(fields[0]))
                    .siteName(intWithNulParse(fields[1]))
                    .posaContinent(intWithNulParse(fields[2]))
                    .userLocationCountry(intWithNulParse(fields[3]))
                    .userLocationRegion(intWithNulParse(fields[4]))
                    .userLocationCity(intWithNulParse(fields[5]))
                    .origDestinationDistance(doubleWithNulParse(fields[6]))
                    .userId(intWithNulParse(fields[7]))
                    .isMobile(intWithNulParse(fields[8]))
                    .isPackage(intWithNulParse(fields[9]))
                    .channel(intWithNulParse(fields[10]))
                    .srchCi(stringWithNulParse(fields[11]))
                    .srchCo(stringWithNulParse(fields[12]))
                    .srchAdultsCnt(intWithNulParse(fields[13]))
                    .srchChildrenCnt(intWithNulParse(fields[14]))
                    .srchRmCnt(stringWithNulParse(fields[15]))
                    .srchDestinationId(intWithNulParse(fields[16]))
                    .srchDestinationTypeId(intWithNulParse(fields[17]))

                    .isBooking(intWithNulParse(fields[18]))
                    .cnt(longWithNulParse(fields[19]))

                    .hotelContinent(intWithNulParse(fields[20]))
                    .hotelCountry(intWithNulParse(fields[21]))
                    .hotelMarket(intWithNulParse(fields[22]))

                    .hotelCluster(intWithNulParse(fields[23]))

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
