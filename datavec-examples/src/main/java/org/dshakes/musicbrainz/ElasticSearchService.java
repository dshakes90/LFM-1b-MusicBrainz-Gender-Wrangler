package org.dshakes.musicbrainz;

import com.google.gson.Gson;
import org.apache.commons.lang.StringEscapeUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

public class ElasticSearchService {

    private static Gson gson = new Gson();

    public static Artist[] SearchArtistName(String query) throws IOException {

        query = URLEncoder.encode(query, "utf8");

        URL urlForGetRequest = new URL("http://localhost:8080/api/_search/artists?query=%22" + query + "%22");
        String readLine = null;
        HttpURLConnection conection = (HttpURLConnection) urlForGetRequest.openConnection();
        conection.setRequestMethod("GET");
        //conection.setRequestProperty("userId", "a1bcdef"); // set userId its a sample here
        int responseCode = conection.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_OK) {
            BufferedReader in = new BufferedReader(
                new InputStreamReader(conection.getInputStream()));
            StringBuffer response = new StringBuffer();
            while ((readLine = in .readLine()) != null) {
                response.append(readLine);
            } in .close();

            Artist[] artists = gson.fromJson(response.toString(), Artist[].class);

//            for (Artist artist : artists) {
//                System.out.println(artist.toString());
//            }
            return artists;

        } else {
            System.out.println("GET NOT WORKED");
        }

        return null;
    }

    public static Artist[] SearchArtistID(String query) throws IOException {

        query = URLEncoder.encode(query, "utf8");

        URL urlForGetRequest = new URL("http://localhost:8080/api/_search/artistsid?query=" + query);
        String readLine = null;
        HttpURLConnection conection = (HttpURLConnection) urlForGetRequest.openConnection();
        conection.setRequestMethod("GET");
        //conection.setRequestProperty("userId", "a1bcdef"); // set userId its a sample here
        int responseCode = conection.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_OK) {
            BufferedReader in = new BufferedReader(
                new InputStreamReader(conection.getInputStream()));
            StringBuffer response = new StringBuffer();
            while ((readLine = in .readLine()) != null) {
                response.append(readLine);
            } in .close();

            Artist[] artists = gson.fromJson(response.toString(), Artist[].class);

//            for (Artist artist : artists) {
//                System.out.println(artist.toString());
//            }
            return artists;

        } else {
            System.out.println("GET NOT WORKED");
        }

        return null;
    }

    public static void main(String[] args) {
        try {
            SearchArtistName("Xiu Xiu / Devendra Banhart split 7");
            SearchArtistName("The Smiths");
            //MyGETRequest("id:100");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
