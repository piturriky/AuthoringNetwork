import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by albertberga on 28/06/2016.
 */
public class ANOutputFormat  extends FileOutputFormat<ANWritable, LongWritable> {

    @Override
    public org.apache.hadoop.mapreduce.RecordWriter<ANWritable, LongWritable> getRecordWriter(TaskAttemptContext arg0) throws IOException, InterruptedException {
        //get the current path
        Path path = FileOutputFormat.getOutputPath(arg0);
        //create the full path with the output directory plus our filename
        Path fullPath = new Path(path, "result.dot");

        //create the file in the file system
        FileSystem fs = path.getFileSystem(arg0.getConfiguration());
        FSDataOutputStream fileOut = fs.create(fullPath, arg0);

        //create our record writer with the new file
        return new ANRecordWriter(fileOut);
    }
}
