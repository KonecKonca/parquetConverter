package com.kozitski.converter.useless;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSFileRead {

    public static void main(String[] args) {

        Configuration conf = new Configuration();
        FSDataInputStream in = null;
        OutputStream out = null;

        try {
            FileSystem fs = FileSystem.get(conf);
            // Input file path
            Path inFile = new Path(args[0]);

            // Check if file exists at the given location
            if (!fs.exists(inFile)) {
                System.out.println("Input file not found");
                throw new IOException("Input file not found");
            }
            // open and read from file
            in = fs.open(inFile);
            //displaying file content on terminal
            out = System.out;
            byte buffer[] = new byte[256];

            int bytesRead = 0;
            while ((bytesRead = in.read(buffer)) > 0) {
                out.write(buffer, 0, bytesRead);
            }

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        finally {
            // Closing streams
            try {
                if(in != null) {
                    in.close();
                }
                if(out != null) {
                    out.close();
                }
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }


    }
}