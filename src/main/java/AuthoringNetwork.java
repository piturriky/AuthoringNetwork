import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by lluis on 28/6/16.
 */
public class AuthoringNetwork extends Configured implements Tool {

    public static class ANMapper extends Mapper<Object, Text, ANWritable, LongWritable> {

        private final LongWritable one = new LongWritable(1);
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            try {
                JSONObject jsonObject = new JSONObject(value.toString());
                JSONArray authors = jsonObject.getJSONArray("authors");
                for (int i = 0; i < authors.length(); i++) {
                    String author = authors.getString(i);
                    for (int j = 0; j < authors.length(); j++) {
                        if (i != j) {
                            context.write(new ANWritable(new Text(author), new Text(authors.getString(j))), one);
                        }
                    }
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
    }

    /*public static class ANWritable implements WritableComparable<ANWritable> {

        private String coAuthor;
        private int weight;

        public ANWritable(String coAuthor, int weight) {
            super();
            this.coAuthor = coAuthor;
            this.weight = weight;
        }

        public ANWritable(){}

        public String getCoAuthor() {
            return coAuthor;
        }

        public void setCoAuthor(String coAuthor) {
            this.coAuthor = coAuthor;
        }

        public int getWeight() {
            return weight;
        }

        public void setWeight(int weight) {
            this.weight = weight;
        }

        @Override
        public int compareTo(ANWritable o) {
            return this.coAuthor.compareTo(o.coAuthor);
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeBytes(coAuthor);
            dataOutput.writeInt(weight);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            coAuthor = dataInput.readUTF();
            weight = dataInput.readInt();
        }
    }*/
/*
    // http://johnnyprogrammer.blogspot.com.es/2012/01/custom-file-output-in-hadoop.html
    public class ANOutputFormat extends FileOutputFormat<Text, ANWritable> {
        public ANOutputFormat() {
        }

        @Override
        public org.apache.hadoop.mapreduce.RecordWriter<Text, ANWritable> getRecordWriter(TaskAttemptContext arg0) throws IOException, InterruptedException {
            //get the current path
            Path path = FileOutputFormat.getOutputPath(arg0);
            //create the full path with the output directory plus our filename
            Path fullPath = new Path(path, "result.txt");

            //create the file in the file system
            FileSystem fs = path.getFileSystem(arg0.getConfiguration());
            FSDataOutputStream fileOut = fs.create(fullPath, arg0);

            //create our record writer with the new file
            return new ANRecordWriter(fileOut);
        }
    }

    public class ANRecordWriter extends RecordWriter<Text, ANWritable> {
        private DataOutputStream out;

        public ANRecordWriter(DataOutputStream stream) {
            out = stream;
            try {
                out.writeBytes("Graph {\r\n");
            }
            catch (Exception ex) {
            }
        }

        @Override
        public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
            out.writeBytes("}");
            //close our file
            out.close();
        }

        @Override
        public void write(Text arg0, ANWritable arg1) throws IOException, InterruptedException {
            out.writeBytes(arg0.toString() + " -- " + arg1.coAuthor + "[label=" + arg1.weight + "]\r\n");
        }
    }*/


    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();

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
