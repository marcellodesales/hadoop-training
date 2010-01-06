// (c) Copyright 2009 Cloudera, Inc.
// Hadoop 0.20.1 API Updated by Marcello de Sales (marcello.desales@gmail.com)
package index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * LineIndexer Creates an inverted index over all the words in a document corpus, mapping each observed word to a list
 * of filename@offset locations where it occurs.
 */
public class WordCountsInDocuments extends Configured implements Tool {

    // where to put the data in hdfs when we're done
    private static final String OUTPUT_PATH = "2-word-counts";

    // where to read the data from.
    private static final String INPUT_PATH = "1-word-freq";

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        Job job = new Job(conf, "Words Counts");

        job.setJarByClass(WordCountsInDocuments.class);
        job.setMapperClass(WordCountsForDocsMapper.class);
        job.setReducerClass(WordCountsForDocsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCountsInDocuments(), args);
        System.exit(res);
    }
}
