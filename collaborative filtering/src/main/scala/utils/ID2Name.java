package utils;


import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ID2Name {
    private static final String videoIdPath = "data/t_video.txt";
    private static final String userIdPath = "data/t_user.txt";
    private Map<String, String> videoIdMapName;
    private Map<String, String> userIdMapName;


    public ID2Name(){
        videoIdMapName = new HashMap<>();
        userIdMapName = new HashMap<>();
        try {
            List<String> videoStringList = FileTools.readFile2List(videoIdPath);
            List<String> userStringList = FileTools.readFile2List(userIdPath);

            for(String s: videoStringList){
                String[] line = s.split("#");
                String videoID = line[0];
                String videoName = line[1];
                videoIdMapName.put(videoID, videoName);
            }
            for (String s: userStringList){
                String[] line = s.split("#");
                String userID = line[0];
                String userName = line[1];
                userIdMapName.put(userID, userName);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Map<String, String> getVideoIdMapName() {
        return videoIdMapName;
    }

    public Map<String, String> getUserIdMapName() {
        return userIdMapName;
    }

}
