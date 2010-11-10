package tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * WordFrequenceInDocument Creates the index of the words in documents, 
 * mapping each of them to their frequency.
 * @author Marcello de Sales (marcello.desales@gmail.com)
 * @version "Hadoop 0.20.1"
 */
public class WordsInCorpusTFIDF extends Configured implements Tool {
    
    // where to put the data in hdfs when we're done
    private static final String OUTPUT_PATH = "1-word-freq";
    
    // where to put the data in hdfs when we're done
    private static final String OUTPUT_PATH_2 = "2-word-counts";

    /* (non-Javadoc)
     * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
     */
    public int run(String[] args) throws Exception {
      
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        Path userInputPath = new Path(args[0]);

        // Remove the user's output path
        Path userOutputPath = new Path(args[1]);
        if (fs.exists(userOutputPath)) {
            fs.delete(userOutputPath, true);
        }

        // Remove the phrase of word frequency path
        Path wordFreqPath = new Path(OUTPUT_PATH);
        if (fs.exists(wordFreqPath)) {
            fs.delete(wordFreqPath, true);
        }

        // Remove the phase of word counts path
        Path wordCountsPath = new Path(OUTPUT_PATH_2);
        if (fs.exists(wordCountsPath)) {
            fs.delete(wordCountsPath, true);
        }

        //Getting the number of documents from the user's input directory.
        FileStatus[] userFilesStatusList = fs.listStatus(userInputPath);
        final int numberOfUserInputFiles = userFilesStatusList.length;
        String[] fileNames = new String[numberOfUserInputFiles];
        for (int i = 0; i < numberOfUserInputFiles; i++) {
            fileNames[i] = userFilesStatusList[i].getPath().getName(); 
        }

        Job job = new Job(conf, "Word Frequence In Document");
        job.setJarByClass(WordFrequenceInDocument.class);
        job.setMapperClass(WordFrequenceInDocMapper.class);
        job.setReducerClass(WordFrequenceInDocReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, userInputPath);
        TextOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        job.waitForCompletion(true);

        Configuration conf2 = getConf();
        conf2.setStrings("documentsInCorpusList", fileNames);
        Job job2 = new Job(conf2, "Words Counts");
        job2.setJarByClass(WordCountsInDocuments.class);
        job2.setMapperClass(WordCountsForDocsMapper.class);
        job2.setReducerClass(WordCountsForDocsReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
        TextOutputFormat.setOutputPath(job2, new Path(OUTPUT_PATH_2));

        job2.waitForCompletion(true);

        Configuration conf3 = getConf();
        conf3.setInt("numberOfDocsInCorpus", numberOfUserInputFiles);
        Job job3 = new Job(conf3, "TF-IDF of Words in Corpus");
        job3.setJarByClass(WordsInCorpusTFIDF.class);
        job3.setMapperClass(WordsInCorpusTFIDFMapper.class);
        job3.setReducerClass(WordsInCorpusTFIDFReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job3, new Path(OUTPUT_PATH_2));
        TextOutputFormat.setOutputPath(job3, userOutputPath);

        return job3.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordsInCorpusTFIDF(), args);
        System.exit(res);
    }
}
