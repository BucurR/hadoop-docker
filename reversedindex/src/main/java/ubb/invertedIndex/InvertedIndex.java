package ubb.invertedIndex;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Set<String> stopWords = new HashSet<>();
        private Text word = new Text();
        private Text docInfo = new Text();
        private String filename;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            Path stopwordsPath = new Path(conf.get("stopwords.path"));
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(stopwordsPath)));
            String stopWord;
            while ((stopWord = br.readLine()) != null) {
                stopWords.add(stopWord.trim().toLowerCase());
            }
            br.close();
            filename = ((FileSplit) context.getInputSplit()).getPath().getName();
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\s+");
            long lineNumber = ((LongWritable) key).get();
            String lineInfo = filename + ":" + lineNumber;

            for (String token : tokens) {
                String normalizedToken = token.toLowerCase();
                if (!normalizedToken.isEmpty() && !stopWords.contains(normalizedToken)) {
                    word.set(normalizedToken);
                    docInfo.set(lineInfo);
                    context.write(word, docInfo);
                }
            }
        }
    }

    public static class LocationReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> locations = new HashSet<>();
            for (Text val : values) {
                locations.add(val.toString());
            }
            result.set(String.join(", ", locations));
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: InvertedIndex <input path> <output path> <stopwords path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("stopwords.path", args[2]);

        Job job = Job.getInstance(conf, "Inverted Index");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(LocationReducer.class);
        job.setReducerClass(LocationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}