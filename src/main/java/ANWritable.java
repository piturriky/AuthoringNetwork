import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by albertberga on 28/06/2016.
 */
public class ANWritable  implements WritableComparable<ANWritable> {

    public Text author1, author2;

    public ANWritable(Text author1, Text author2) {
        this.author1 = author1;
        this.author2 = author2;
    }

    public ANWritable(){
        this.author1 = new Text();
        this.author2 = new Text();
    }

    @Override
    public int compareTo(ANWritable o) {
        int compareResult = author1.compareTo(o.author2) + author2.compareTo(o.author1);

        return compareResult == 0 ? compareResult : author1.compareTo(o.author1) + author2.compareTo(o.author2);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        author1.write(dataOutput);
        author2.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        author1.readFields(dataInput);
        author2.readFields(dataInput);
    }
}
