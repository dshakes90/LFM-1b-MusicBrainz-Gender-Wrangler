package org.dshakes.musicbrainz;
import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

public class PreProcessLE {

    private static String LFMPath = "/mnt/usb-WD_Elements_25A2_57584D314536394539594354-0:0-part1/Data/LFM-1b";
    private static String LEFile = "/LFM-1b_LEs.txt";
    private static String LEPath = LFMPath + LEFile;

    private static int undefCount = 0;
    private static int numRowsRead = 0;
    private static int ambiguousCount = 0;
    private static int noQuery = 0;
    private static int[] totalGenderCounts = new int[5];

    private static String url = "jdbc:postgresql://localhost:5432/musicbrainz";
    private static String user = "musicbrainz";
    private static String password = "musicbrainz";
    private static final Connection connection = GetConnection();

    private static String querySongLFM = "SELECT name FROM lastfm.songs WHERE artistid = ?";

    private static PreparedStatement ps1;
    private static PreparedStatement psSongLFM;
    private static PreparedStatement psSongMB;

    private static ElasticSearchService elasticSearch = new ElasticSearchService();

    /*
    static {
        try {
            //ps1 = connection.prepareStatement(query1);
            //psSongLFM = connection.prepareStatement(querySongLFM);
            //psSongMB = connection.prepareStatement(querySongMB);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

     */

    private static Connection GetConnection() {
        try {
            return DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String args[]){
        preProcessLEPath(LEPath);

    }


    public static void preProcessLEPath(String path){
        // user-id, artist-id, album-id, track-id, timestamp
        int userID;
        int artistID;
        int albumID;
        int trackID;
        int timestamp;


        try{
            BufferedReader buf = new BufferedReader(new FileReader(path));
            ArrayList<String> words = new ArrayList<>();
            String lineJustFetched = null;
            String[] wordsArray;

            while(true){
                lineJustFetched = buf.readLine();
                if(lineJustFetched == null){
                    break;
                }else{
                    wordsArray = lineJustFetched.split("\t");

                    userID = Integer.parseInt(wordsArray[0]);
                    artistID = Integer.parseInt(wordsArray[1]);
                    albumID = Integer.parseInt(wordsArray[2]);
                    trackID = Integer.parseInt(wordsArray[3]);
                    timestamp = Integer.parseInt(wordsArray[4]);

                    System.out.println(wordsArray[0]);

                    /*
                    for(String each : wordsArray){
                        if(!"".equals(each)){
                            words.add(each);
                        }
                    }
                     */
                }
            }
            buf.close();

        }catch(Exception e){
            e.printStackTrace();
        }
    }

}
