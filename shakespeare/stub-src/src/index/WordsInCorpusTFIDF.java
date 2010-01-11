package index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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
    private static final String OUTPUT_PATH = "3-tf-idf";

    // where to read the data from.
    private static final String INPUT_PATH = "2-word-counts";

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        Job job = new Job(conf, "Word in Corpus, TF-IDF");

        job.setJarByClass(WordsInCorpusTFIDF.class);
        job.setMapperClass(WordsInCorpusTFIDFMapper.class);
        job.setReducerClass(WordsInCorpusTFIDFReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        //Getting the number of documents from the original input directory.
        Path inputPath = new Path("input");
        FileSystem fs = inputPath.getFileSystem(conf);
        FileStatus[] stat = fs.listStatus(inputPath);
        
        //Dirty hack to pass the total number of documents as the job name. 
        //The call to context.getConfiguration.get("docsInCorpus") returns null when I tried to pass
        //conf.set("docsInCorpus", String.valueOf(stat.length)) Or even
        //conf.setInt("docsInCorpus", stat.length)
        job.setJobName(String.valueOf(stat.length));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordsInCorpusTFIDF(), args);
        System.exit(res);
    }
}
