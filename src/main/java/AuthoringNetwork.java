import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.util.*;

/**
 * Created by lluis on 28/6/16.
 */
public class AuthoringNetwork extends Configured implements Tool {

    public static class FirstMapper extends Mapper<Object, Text, ANWritable, LongWritable> {
        private Set<String> coauthors = new HashSet<>();
        private String analysedAuthor = "";

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            analysedAuthor = context.getConfiguration().get("author");
        }

        private final LongWritable one = new LongWritable(1);
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            try {
                JSONObject jsonObject = new JSONObject(value.toString());
                JSONArray authors = jsonObject.getJSONArray("authors");
                int analysedAuthorPosition = -1;
                for (int i = 0; i < authors.length(); i++) {
                    String author = authors.getString(i);
                    for (int j = i + 1; j < authors.length(); j++) {
                        if (i != j) {
                            context.write(new ANWritable(new Text(author), new Text(authors.getString(j))), one);
                        }
                    }
                    if(author.equals(analysedAuthor))
                        analysedAuthorPosition = i;
                }

                if(analysedAuthorPosition != -1)
                    for (int i = 0; i < authors.length(); i++) {
                        if(i != analysedAuthorPosition)
                            coauthors.add(authors.getString(i));
                    }
            } catch (JSONException e) {
                    e.printStackTrace();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.getConfiguration().setStrings("coauthors", coauthors.toArray(new String[coauthors.size()]));
            context.getConfiguration().setStrings("test", "A", "B", "C");
        }
    }

    public static class SecondMapper extends Mapper<ANWritable, LongWritable, ANWritable, LongWritable> {
        
        private final LongWritable one = new LongWritable(1);
        private Collection<String> coauthors;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            coauthors = context.getConfiguration().getStringCollection("coauthors");
            for(String s: coauthors)
                System.out.println(s);


            Collection<String> test = context.getConfiguration().getStringCollection("test");
            for(String s: test)
                System.out.println(s);
        }

        public void map(ANWritable key, LongWritable value, Context context)
                throws IOException, InterruptedException {

            if(coauthors.contains(key.author1) && coauthors.contains(key.author2))
                context.write(key, value);
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.printf("Usage: %s <Input file> <Output file> <Author name>\n",
                    getClass().getSimpleName());
            return -1;
        }

        Configuration conf = getConf();

        conf.set("author", args[2]);

        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf);



        Configuration firstMapperConfig = new Configuration(false);
        ChainMapper.addMapper(job, FirstMapper.class, Object.class,Text.class, ANWritable.class, LongWritable.class, firstMapperConfig);

        Configuration secondMapperConfig = new Configuration(false);
        ChainMapper.addMapper(job, SecondMapper.class, ANWritable.class, LongWritable.class, ANWritable.class, LongWritable.class, secondMapperConfig);

//        job.setMapperClass(FirstMapper.class);
        job.setJarByClass(AuthoringNetwork.class);
        job.setReducerClass(LongSumReducer.class);

        job.setOutputKeyClass(ANWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(ANOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AuthoringNetwork(), args);
        System.exit(exitCode);  }
}
