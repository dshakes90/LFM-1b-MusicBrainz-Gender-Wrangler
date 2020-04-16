package org.dshakes.musicbrainz;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DeriveGenderFromDb {

    // Flag to determin which dataset we are using
    private static boolean oldLFM1b = true;

    private static int undefCount = 0;
    private static int numRowsRead = 0;
    private static int ambiguousCount = 0;
    private static int noQuery = 0;
    private static int[] totalGenderCounts = new int[5];

    private static String url = "jdbc:postgresql://localhost:5432/musicbrainz";
    private static String user = "musicbrainz";
    private static String password = "musicbrainz";
    private static final Connection connection = GetConnection();

    private static String queryMBID = "SELECT gender FROM lastfm.artists WHERE grid = ?";

    private static String querySongLFM = "SELECT name FROM lastfm.songs WHERE artistid = ?";
    private static String querySongMB = "SELECT name FROM track WHERE artist_credit = ?";
    private static String query1 = "SELECT entity0, entity1 FROM L_artist_artist WHERE " +
            "(entity0 = ? OR entity1 = ?) AND link IN (SELECT id FROM link WHERE link_type = 103)";

    private static PreparedStatement ps1;
    private static PreparedStatement psSongLFM;
    private static PreparedStatement psSongMB;

    private static PreparedStatement psMBIDArtistName;

    private static ElasticSearchService elasticSearch = new ElasticSearchService();

    static {
        try {
            ps1 = connection.prepareStatement(query1);
            psSongLFM = connection.prepareStatement(querySongLFM);
            psSongMB = connection.prepareStatement(querySongMB);

            // newly added prepared statment
            psMBIDArtistName = connection.prepareStatement(queryMBID);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static Connection GetConnection() {
        try {
            return DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static int getAmbiguousCount() {
        return ambiguousCount;
    }

    public static int[] getTotalGenderCounts() {
        return totalGenderCounts;
    }

    public static int getNumRowsRead() {
        return numRowsRead;
    }

    public static int getUndefCount() { return undefCount; }

    public static int getNoQuery() {
        return noQuery;
    }

    private static synchronized void incUndefCount() {
        undefCount++;
    }

    private static synchronized void incCounter() {
        if (numRowsRead % 100 == 0) {
            System.out.println("rows read: " + numRowsRead);
        } numRowsRead++;
    }

    private static int getIndexWithMaxVal(int[] a){
        int maxIndex = 0;
        for (int i = 0; i < a.length; i++) {
            maxIndex = a[i] > a[maxIndex] ? i : maxIndex;
        }
        return maxIndex;
    }

    // Find gender with majority in local count
    private static synchronized void incGlobalGenderCount(int[] localGenderCount) {
        totalGenderCounts[getIndexWithMaxVal(localGenderCount)]++;
    }

    private static void updateDebugCounts(int[] genderCounts) {
        if(getIndexWithMaxVal(genderCounts) == 0){
            incUndefCount();
        }
    }

    // Checks quantity of songs for artist in lfm-1b which exist in returned mb result set
    private static List matchSongs(List<String> lfmSongNames, List<String> mbSongNames) {
        lfmSongNames.retainAll(mbSongNames);
        return lfmSongNames;
    }

    private static int getAmbiguousArtistSongSimilarity(int lfmId, int mbId) {

        List<String> lfmSongNames = new ArrayList<>();
        List<String> mbSongNames = new ArrayList<>();

        try {
            psSongMB.setInt(1, mbId);
            ResultSet rsSongsMB = psSongMB.executeQuery();

            psSongLFM.setInt(1, lfmId);
            ResultSet rsLFMSongs = psSongLFM.executeQuery();

            while (rsLFMSongs.next()) {
                lfmSongNames.add(rsLFMSongs.getString("name"));
            }
            rsLFMSongs.close();

            while (rsSongsMB.next()) {
                mbSongNames.add(rsSongsMB.getString("name"));
            }
            rsSongsMB.close();

            // check and return similarity
            return matchSongs(lfmSongNames, mbSongNames).size();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    // Queries gender of all artists in the band and increments gender count for all member genders
    private static int[] getBandGender(Integer id) {

        Integer bandMemberID;
        int[] genderCounts = new int[5];

        try {
            ps1.setInt(1, id);
            ps1.setInt(2, id);

            // retrieve all members from the band
            ResultSet rs1 = ps1.executeQuery();

            while (rs1.next()) {
                Integer bandMembergender = 0;
                bandMemberID = (Integer) rs1.getObject("entity0");

                try {
                    Artist[] artists = elasticSearch.SearchArtistID(String.valueOf(bandMemberID));

                    if (artists.length > 0) {
                        bandMembergender = artists[0].getGender();

                        if (bandMembergender == null) {
                            bandMembergender = 0;
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }

                genderCounts[bandMembergender]++;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return genderCounts;
    }

    // Checks if all values in an array are zero
    private static boolean areAllZero(int[] array) {
        for (int n : array) if (n != 0) return false;
        return true;
    }

    // Checks if artist is a band and classifies gender
    private static int[] classifyGender(Integer gender, Integer mbId, int[] genderCounts, Integer type) {

        int[] localGenderCounts = Arrays.copyOf(genderCounts, genderCounts.length);

        // considering band - compute genders of members
        if (type != null && type == 2) {
            localGenderCounts = getBandGender(mbId);
            if (areAllZero(localGenderCounts)) {
                localGenderCounts[0]++;
            }
        } else {
            // considering individual artist
            if (gender != null) {
                localGenderCounts[gender]++;
            } else {
                localGenderCounts[0]++;
            }
        }
        return localGenderCounts;
    }

    // Splits artist name for a given delimiter
    private static String[] splitArtistOnDelimiter(String delimiter, String artistName) {
        return artistName.split(delimiter);
    }

    // Chooses artist from candidate artists with max unison of songs in mb db and LFM-1b
    private static Integer[] chooseArtistWithMaxSongSimilarity(Artist[] artists, Integer lfmID) {

        Integer[] artistMetaData = new Integer[3]; // contains gender, type and mbID
        int maxTrackSimilarity = 0;
        int trackSimilarity;

        for (Artist artist : artists) {

            Integer elasticMBId = Math.toIntExact(artist.getId());
            trackSimilarity = getAmbiguousArtistSongSimilarity(lfmID, elasticMBId);

            // if artist shares most similar songs, update found artist
            if (trackSimilarity > maxTrackSimilarity) {
                artistMetaData[0] = artist.getGender();
                artistMetaData[1] = artist.getType();
                artistMetaData[2] = elasticMBId;
                maxTrackSimilarity = trackSimilarity;
            }
        }
        return artistMetaData;
    }


    // Chooses artist from candidates and classifies gender of given artist
    private static int[] chooseArtistFromCandidates(Artist[] artists, int lfmID, int[] genderCounts){

        // Choose candidate artist based off which ever artist shares the most song matches to LFM-1b data-set
        Integer[] artistMetaData = chooseArtistWithMaxSongSimilarity(artists, lfmID);

        return classifyGender(artistMetaData[0], artistMetaData[2], genderCounts , artistMetaData[1]);
    }

    /*
     * Gender classifications key: *
     *
     * - 0 = undef / ambiguous
     * - 1 = male
     * - 2 = female
     * - 3 = other
     * - 4 = not applicable
     * - genderCounts: undefCount / maleCount / femaleCount / otherCount / naCount
     */

    public static String getArtistGender(String artistName, Integer lfmID, String mbIDhash) {

        Integer gender;
        Integer type;
        Integer mbID;
        String[] splitArtistNames;
        Artist[] artists;
        int[] genderCounts = new int[]{0, 0, 0, 0, 0};

        incCounter();

        try {

            // hacky but might work, (1) retirve artist name via hashs
            if(oldLFM1b == true){
                String tempArtistName = null;
                psMBIDArtistName.setString(1, mbIDhash);
                ResultSet rsArtistName = psMBIDArtistName.executeQuery();

                while (rsArtistName.next()) {
                    tempArtistName = rsArtistName.getString("name");
                } rsArtistName.close();

                if (tempArtistName != null){
                    artistName = tempArtistName;
                }
            }


            // if we are working with the other LastFm dataset, then first query the mbid,
            artists = elasticSearch.SearchArtistName(artistName);

            // (1) We have an exact match on the name, no need to do any more logic to find artist
            if (artists.length > 0 && artists[0].getName().equals(artistName)) {

                mbID = Math.toIntExact(artists[0].getId());
                gender = artists[0].getGender();
                type = artists[0].getType();

                genderCounts = classifyGender(gender, mbID, genderCounts, type);

            } else {

                // (2) We have multiple candidate artists, select one
                genderCounts = chooseArtistFromCandidates(artists, lfmID, genderCounts);

                splitArtistNames = splitArtistOnDelimiter("&", artistName);

                // if majority gender of artist has no known gender, try logic with delimiter
                if (splitArtistNames.length > 1 && getIndexWithMaxVal(genderCounts) == 0) {
                    genderCounts[0] = 0; // reset undef count

                    for (String n : splitArtistNames) {
                        genderCounts = chooseArtistFromCandidates(elasticSearch.SearchArtistName(n), lfmID, genderCounts);
                    }
                }
            }

            // Update global gender and debug counts
            incGlobalGenderCount(genderCounts);
            updateDebugCounts(genderCounts);

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        //System.out.println(genderCounts[0] + "/" + genderCounts[1] + "/" + genderCounts[2] + "/" + genderCounts[3] + "/" + genderCounts[4]);

        return genderCounts[0] + "/" + genderCounts[1] + "/" + genderCounts[2] + "/" + genderCounts[3] + "/" + genderCounts[4];
    }
}