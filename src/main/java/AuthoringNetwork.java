import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
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

    public static class ANMapper extends Mapper<Object, Text, ANWritable, LongWritable> {
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
                for(int i = 0; i < authors.length(); i++)
                    if(authors.getString(i).equals(analysedAuthor)) {
                        analysedAuthorPosition = i;
                        break;
                    }
                if(analysedAuthorPosition == -1) return;

                System.out.println("POSITION -- " + analysedAuthorPosition);

                for (int i = 0; i < authors.length(); i++) {
                    if(i == analysedAuthorPosition) continue;
                    String author = authors.getString(i);

                    System.out.println("EXTERNAL -- " + author);
                    for (int j = i + 1; j < authors.length(); j++) {
                        if(j == analysedAuthorPosition) continue;

                        System.out.println("INTERNAL -- " + authors.getString(j));
                        context.write(new ANWritable(new Text(author), new Text(authors.getString(j))), one);
                    }
                }
            } catch (JSONException e) {
                    e.printStackTrace();
            }
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

        job.setMapperClass(ANMapper.class);
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
