import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created by albertberga on 28/06/2016.
 */
public class ANRecordWriter  extends RecordWriter<ANWritable, LongWritable> {
    private DataOutputStream out;

    public ANRecordWriter(DataOutputStream stream) {
        out = stream;
        try {
            out.writeBytes("graph {\r\n");
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
    public void write(ANWritable relation, LongWritable weight) throws IOException, InterruptedException {
        if(relation.author1.getLength() != 0 && relation.author2.getLength() != 0)
            out.writeBytes("\"" + relation.author1 + "\" -- \"" + relation.author2 + "\" [label=" + weight + "]\r\n");
    }
}
