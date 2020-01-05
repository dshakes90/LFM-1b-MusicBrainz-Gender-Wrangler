package org.dshakes.musicbrainz;

import scala.Int;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DeriveGenderFromDb {

    private static int undefCount = 0;
    private static int numRowsRead = 0;
    private static int ambiguousCount = 0;
    private static int missedBandCount = 0;
    private static int noQuery = 0;
    private static int[] totalGenderCounts = new int[5];

    private static String url = "jdbc:postgresql://localhost:5432/musicbrainz";
    private static String user = "musicbrainz";
    private static String password = "musicbrainz";
    private static final Connection connection = GetConnection();

    private static String queryWildCard = "SELECT id, gender, type FROM artist WHERE UPPER(name) like ?";
    private static String querySongLFM = "SELECT name FROM lastfm.songs WHERE artistid = ?";
    private static String querySongMB = "SELECT name FROM track WHERE artist_credit = ?";

    private static String query = "SELECT id, gender, type FROM artist WHERE UPPER(name) = ?";
    private static String query1 = "SELECT entity0, entity1 FROM L_artist_artist WHERE " +
        "(entity0 = ? OR entity1 = ?) AND link IN (SELECT id FROM link WHERE link_type = 103)";
    private static String query2 = "SELECT name, gender FROM artist WHERE id = ?";

    private static PreparedStatement ps;
    private static PreparedStatement ps1;
    private static PreparedStatement ps2;
    private static PreparedStatement psWild;
    private static PreparedStatement psSongLFM;
    private static PreparedStatement psSongMB;

    private static ElasticSearchService elasticSearch = new ElasticSearchService();

    static {
        try {
            ps = connection.prepareStatement(query);
            ps1 = connection.prepareStatement(query1);
            ps2 = connection.prepareStatement(query2);
            psWild = connection.prepareStatement(queryWildCard);
            psSongLFM = connection.prepareStatement(querySongLFM);
            psSongMB = connection.prepareStatement(querySongMB);

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

    public static int getMissedBandCount() {
        return missedBandCount;
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

    public static int GetUndefCount() { return undefCount; }

    public static int GetNoQuery() {
        return noQuery;
    }

    private static synchronized void incUndefCount() {
        undefCount++;
    }

    private static synchronized void incNoQuery() {
        noQuery++;
    }

    private static synchronized void incCounter() {
        if (numRowsRead % 100 == 0) {
            System.out.println("rows read: " + numRowsRead);
        }
        numRowsRead++;
    }

    // Finds gender with majority in local count
    private static synchronized void incGlobalGenderCount(int[] localGenderCount) {

        int maxIndex = 0;
        for (int i = 0; i < localGenderCount.length; i++) {
            maxIndex = localGenderCount[i] > localGenderCount[maxIndex] ? i : maxIndex;
        }

        totalGenderCounts[maxIndex]++;
    }

    private static void updateDebugCounts(int[] genderCounts) {
        // if we have majority undef in our classification, increment this count
        if (genderCounts[0] >= 1 && genderCounts[0] > genderCounts[1] &&
            genderCounts[0] > genderCounts[2] && genderCounts[0] > genderCounts[3] &&
            genderCounts[0] > genderCounts[4]) {
            incUndefCount();
        }
    }

    // Checks quantity of songs for artist in lfm-1b which exist in returned mb result set
    private static List matchSongs(List<String> lfmSongNames, List<String> mbSongNames) {
        lfmSongNames.retainAll(mbSongNames);
        return lfmSongNames;
    }

    // Less strict logic for computing artist gender from MusicBrainz db using wildcards.
    private static Integer[] getArtistMetaWildCard(String lfmArtistName, Integer lfId) {

        Integer[] wildCardMetaData = new Integer[3];
        String lfmArtistNameWildC = lfmArtistName + "%";

        try {
            psWild.setString(1, lfmArtistNameWildC.toUpperCase());    // result set of artists from wild card query
            ResultSet rsWild = psWild.executeQuery();                   // execute query, returns id, gender, type

            while (rsWild.next()) {
                Integer mbId = rsWild.getInt("id");                  // retrieve id of artist returned by wild card

                if (getAmbiguousArtistSongSimilarity(lfId, mbId) >= 2) {

                    // update artist Meta data
                    Integer wildGender = rsWild.getInt("gender");
                    if (wildGender == null) {
                        wildCardMetaData[0] = 0;
                    } else {
                        wildCardMetaData[0] = wildGender;
                    }

                    wildCardMetaData[1] = rsWild.getInt("type");
                    wildCardMetaData[2] = rsWild.getInt("id");

                    return wildCardMetaData;
                }
            }
            rsWild.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return wildCardMetaData;
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

            //System.out.println("lfm songs: " + lfmSongNames.size());
            //System.out.println("mb songs: " + mbSongNames.size());

            // check and return similarity
            return matchSongs(lfmSongNames, mbSongNames).size();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    private static int[] GetBandGender(Integer id) {

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

                    // System.out.println(artists[0].toString());
                    if (artists.length > 0) {
                        bandMembergender = artists[0].getGender();

                        if(bandMembergender == null){
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

    private static int[] classifyGender(Integer gender, Integer mbId, int[] localGenderCounts, Integer type) {

        // we must be considering a band compute subsequent genders of members
        if (type != null && type == 2) {
            localGenderCounts = GetBandGender(mbId);
        } else {
            // we are considering an individual artist
            if (gender != null) {
                localGenderCounts[gender]++;
            } else {
                localGenderCounts[0]++;
            }
        }
        return localGenderCounts;
    }

    /*
     * Gender classifications key:
     *
     * 0 = undef / ambiguous
     * 1 = male
     * 2 = female
     * 3 = other
     * 4 = not applicable
     *
     * localGenderCounts: undefCount / maleCount / femaleCount / otherCount / naCount
     */

    public static String GetArtistGender(String artistName, Integer lfmID) {

        Integer gender = 0;
        Integer type = 0;
        Integer mbID = 0;

        int[] genderCounts = new int[5];
        int maxTrackSimilarity = 0;
        int trackSimilarity;

        incCounter();

        try {

            Artist[] artists = elasticSearch.SearchArtistName(artistName);

            if (artists.length > 0 && artists[0].getName().equals(artistName)) {

                mbID = Math.toIntExact(artists[0].getId());
                gender = artists[0].getGender();
                type = artists[0].getType();

            } else {

                if (artists.length == 0) {
                    incNoQuery();
                }

                for (Artist artist : artists) {

                    // System.out.println("JSON String Result " + artist.toString());

                    Integer elasticMBId = Math.toIntExact(artist.getId());
                    trackSimilarity = getAmbiguousArtistSongSimilarity(lfmID, elasticMBId);

                    // if artist shares most similar songs, update found artist
                    if (trackSimilarity > maxTrackSimilarity) {
                        mbID = elasticMBId;
                        gender = artist.getGender();
                        type = artist.getType();
                        maxTrackSimilarity = trackSimilarity;
                    }
                }
            }


            /*
            ps.setString(1, artistName.toUpperCase());
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {

                // retrieve artist meta-data
                gender = (Integer) rs.getObject("gender");
                type = (Integer) rs.getObject("type");
                mbID = rs.getInt("id");

                trackSimilarity = getAmbiguousArtistSongSimilarity(lfmID, mbID);

                //System.out.println("Matched songs:" + trackSimilarity);

                if (trackSimilarity > maxTrackSimilarity) {
                    ambigArtistGender = gender;
                    ambigArtistType = type;
                    ambigArtistMbID = mbID;
                    maxTrackSimilarity = trackSimilarity;
                }

                rsSize++;
            } rs.close();

            // (2) if ambiguous artist, update gender to be artists with biggest proportion matched songs.
            if (rsSize >= 2) {
                gender = ambigArtistGender;
                type = ambigArtistType;
                mbID = ambigArtistMbID;
                if (gender != null && gender != 0) {
                    ambigSuccess = true;
                }
            }

            // (3) if our query did not return artists, try less strict condition with wild cards
            //      commend out for now


                // update meta-data found with wild cards
                Integer[] wildCardMetaData = getArtistMetaWildCard(artistName, lfmID);

                gender = wildCardMetaData[0];
                type = wildCardMetaData[1];
                mbID = wildCardMetaData[2];

                if (gender != null && gender != 0) {
                    wildCardSuccess = true;
                }

             */

            // (4)  CONSIDER possible scenarios for which band might be classified
            genderCounts = classifyGender(gender, mbID, genderCounts, type);

            // (5) Update global gender and debug counts
            incGlobalGenderCount(genderCounts);
            updateDebugCounts(genderCounts);

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return genderCounts[0] + "/" + genderCounts[1] + "/" + genderCounts[2] + "/" + genderCounts[3] + "/" + genderCounts[4];
    }
}
