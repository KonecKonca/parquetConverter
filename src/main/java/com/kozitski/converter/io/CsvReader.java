package com.kozitski.converter.io;

import com.kozitski.converter.domain.Test;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class CsvReader {
    private static final String READING_FILE_PATH = "/user/maria_dev/test/text.txt";

    public static void main(String[] args) {
        List<Test> result = new ArrayList<>();

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

    public List<Test> readAll(){
        List<Test> result = new ArrayList<>();

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

        return result;
    }

}
