// (c) Copyright 2009 Cloudera, Inc.
// Hadoop 0.20.1 API Updated by Marcello de Sales (marcello.desales@gmail.com)

package tfidf;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * LineIndexMapper Maps each observed word in a line to a (filename@offset) string.
 * @author Marcello de Sales (mdesales)
 */
public class WordCountsForDocsMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private Text docName = new Text();
    private Text wordAndCount = new Text();
    private static final String[] empty = new String[0];
    
    public WordCountsForDocsMapper() {
    }
    
    /**
     * @param key is the byte offset of the current line in the file;
     * @param value is the line from the file
     * @param context
     * 
     *     PRE-CONDITION: aa@leornardo-davinci-all.txt    1
     *                    aaron@all-shakespeare   98
     *                    ab@leornardo-davinci-all.txt    3
     * 
     *     POST-CONDITION: Output <"all-shakespeare", "aaron=98"> pairs
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] wordAndDocCounter = value.toString().split("\t");
        String[] wordAndDoc = wordAndDocCounter[0].split("@");
        this.docName.set(wordAndDoc[1]);
        this.wordAndCount.set(wordAndDoc[0] + "=" + wordAndDocCounter[1]);
        context.write(this.docName, this.wordAndCount);

        // Get the entire list of documents in corpus from the configuration. If the word does not have appearance, print 0.
        String[] documentsInCorpus = context.getConfiguration().getStrings("documentsInCorpusList", empty);
        for (String document : documentsInCorpus) {
            if (document.equals(wordAndDoc[1])) {
                continue;
            }
            this.docName.set(document);
            this.wordAndCount.set(wordAndDoc[0] + "=0");
            context.write(this.docName, this.wordAndCount);
        }
    }
}
