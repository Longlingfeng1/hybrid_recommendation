package dataprocess;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import utils.FileTools;
import utils.FileTools.*;


public class DataGenerator {

    static String readFilePath = "data/raw/score_nolimit.txt";
    static String writeFilePath = "data/rawLog/score_nolimit_log.txt";

    public static void main(String[] args) throws IOException {

        List<String> fileContent = FileTools.readFile2List(readFilePath);
        List<String> toWriteList = new ArrayList<>();

        for (String line:fileContent){
            String[] res = line.split(",");
            Double score = Math.log( 1.0 + Double.valueOf(res[2]));
            String content = Integer.valueOf(res[0]) + "," + Integer.valueOf(res[1])+","+score;
            toWriteList.add(content);
        }

        FileTools.writeList(writeFilePath,toWriteList,"UTF-8");

    }
}
