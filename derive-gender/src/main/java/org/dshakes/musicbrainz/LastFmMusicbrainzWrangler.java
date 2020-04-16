package org.dshakes.musicbrainz;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.datavec.api.records.reader.impl.csv.CSVRecordReader;
import org.datavec.api.transform.TransformProcess;
import org.datavec.api.transform.condition.ConditionOp;
import org.datavec.api.transform.condition.column.StringColumnCondition;
import org.datavec.api.transform.filter.ConditionFilter;
import org.datavec.api.transform.schema.Schema;
import org.datavec.api.writable.Writable;
import org.datavec.spark.transform.SparkTransformExecutor;
import org.datavec.spark.transform.misc.StringToWritablesFunction;
import org.datavec.spark.transform.misc.WritablesToStringFunction;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

public class LastFmMusicbrainzWrangler {

    private static boolean LastFmOld = true;

    public static void main(String[] args)throws Exception {

        // RENAME PATH TO USERS PATH:
        //String baseDir = "/home/dshakes/Desktop/TestCSV/";
        //String fileName = "LFM-1b_artists_strip.txt";

        // Test on new last-fm dataset
        String baseDir = "/media/dshakes/Elements1/lastfm-dataset-360K_0.2/lastfm-dataset-360K/out/";
        String fileName = "lastfm360k_artists.txt";
        //String fileName = "test-artists.txt";
        //String fileName = "LFM-trim-test.txt";
        String delimiter = "\t";

        int numLinesToSkip = 0;

        String inputPath = baseDir + fileName;
        String timeStamp = String.valueOf(new Date().getTime());
        String outputPath = baseDir + "LFM-1b_artists_" + timeStamp;

        Schema lastFmArtistSchema = new Schema.Builder()
            .addColumnInteger("artist_id")
            .addColumnString("artist").addColumnString("mdid")
            .build();

        TransformProcess tp = new TransformProcess.Builder(lastFmArtistSchema)
            .transform(new DeriveGenderFromArtistNameTransform.Builder("name")
                .addStringDerivedColumn("gender")
                .build())
            // will need to remove / add a count to produce statistics on accuracy
            .filter(new ConditionFilter(
                new StringColumnCondition("gender", ConditionOp.InSet, new HashSet<>(Arrays.asList("0","5", "3", "9")))))
            .build();

        int numActions = tp.getActionList().size();
        for (int i = 0; i<numActions; i++){
            System.out.println("\n\n===============================");
            System.out.println("--- Schema after step " + i +
                " (" + tp.getActionList().get(i) + ")--" );
            System.out.println(tp.getSchemaAfterStep(i));
        }

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("LastFM Record Reader Transform");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // read the data file
        JavaRDD<String> lines = sc.textFile(inputPath);

        // convert to Writable
        JavaRDD<List<Writable>> artists = lines.map(new StringToWritablesFunction(new CSVRecordReader(numLinesToSkip, '\t')));
        // run our transform process
        JavaRDD<List<Writable>> processed = SparkTransformExecutor.execute(artists,tp);
        // convert Writable back to string for export
        JavaRDD<String> toSave= processed.map(new WritablesToStringFunction(delimiter));

        toSave.coalesce(1).saveAsTextFile(outputPath);

        // toSave.saveAsTextFile(outputPath);
        int[] genderCounts = DeriveGenderFromDb.getTotalGenderCounts();
        double undefs = DeriveGenderFromDb.getUndefCount();
        long totalArtists = artists.count();

        // ----------------------------- PRINT STATS -----------------------------
        System.out.println("- total artists : " + totalArtists);
        System.out.println("- Global undef count: " + undefs/totalArtists);
    }
}
