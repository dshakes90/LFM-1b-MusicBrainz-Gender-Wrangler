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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

public class LastFmMusicbrainzWrangler {

    public static void main(String[] args)throws Exception {

        int numLinesToSkip = 0;
        String delimiter = "\t";
        String baseDir = "/home/dshakes/Desktop/TestCSV/";
        // String fileName = "test2.csv";
        String statsFileName = "LFM-1b_artist_stats2.txt";

        // String fileName = "LFM-1b-test.txt";
        //String fileName = "LFM-trim-test.txt";

        String fileName = "LFM-1b_artists_strip.txt";

        String inputPath = baseDir + fileName;
        String timeStamp = String.valueOf(new Date().getTime());
        String outputPath = baseDir + "LFM-1b_artists_" + timeStamp;


        Schema lastFmArtistSchema = new Schema.Builder()
            .addColumnInteger("artist_id")
            .addColumnString("name")
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

        int ambiguous = DeriveGenderFromDb.getAmbiguousCount();
        int missedBand = DeriveGenderFromDb.getMissedBandCount();
        Integer numRowsRead = DeriveGenderFromDb.getNumRowsRead();
        int[] genderCounts = DeriveGenderFromDb.getTotalGenderCounts();

        long totalArtists = artists.count();

        // write stats to tsv file to be analysed in jupyter notebook
        tsvWriter(baseDir+statsFileName, genderCounts, numRowsRead, missedBand, ambiguous);

        System.out.println("total artists : " + totalArtists);
        System.out.println("num rows read : " + numRowsRead);

        double nulls = genderCounts[0];
        double undefs = DeriveGenderFromDb.GetUndefCount();

        // looking for a value around: 940603
        //                             168459
        //                            2849211

        // PRINT STATS
        System.out.println("- Nulls found: " + nulls/totalArtists);
        System.out.println("- Global undef count: " + undefs/totalArtists);
        System.out.println("- Errors due to no results (could be solved by wild cards): " + DeriveGenderFromDb.GetNoQuery());
        System.out.println("- Ambiguous artist logic success:" + DeriveGenderFromDb.getAmbiguousCount());

        /*
         * 3190371
         * 2680752
         */
    }

    private static void tsvWriter(String path, int[] genderDist, Integer count, int missedBand, int ambiguous){
        try (PrintWriter writer = new PrintWriter(new File(path))) {

            StringBuilder sb = new StringBuilder();
            sb.append("Undef");
            sb.append('\t');
            sb.append("Male");
            sb.append('\t');
            sb.append("Female");
            sb.append('\t');
            sb.append("Other");
            sb.append('\t');
            sb.append("NA");
            sb.append('\t');
            sb.append("Ambiguous");
            sb.append('\t');
            sb.append("Total");
            sb.append('\n');

            for (Integer gd : genderDist){
                sb.append(gd);
                sb.append('\t');
            }

            sb.append(ambiguous);
            sb.append('\t');
            sb.append(count);
            sb.append('\t');

            writer.write(sb.toString());

        } catch (FileNotFoundException e) {
            System.out.println(e.getMessage());
        }
    }

}
