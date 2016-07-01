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
        if(author1.compareTo(author2) > 0) {
            this.author1 = author1;
            this.author2 = author2;
        }else{
            this.author1 = author2;
            this.author2 = author1;
        }
    }

    public ANWritable(){
        this.author1 = new Text();
        this.author2 = new Text();
    }

    @Override
    public int compareTo(ANWritable o) {
//        boolean crossEquals = author1.equals(o.author2) && author2.equals(o.author1);

        return /*crossEquals ? 0 : */(author1.toString()+author2.toString()).compareTo((o.author1.toString()+o.author2.toString()));
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ANWritable that = (ANWritable) o;

        if (author1 != null ? !author1.equals(that.author1) : that.author1 != null) return false;
        return author2 != null ? author2.equals(that.author2) : that.author2 == null;

    }

    @Override
    public int hashCode() {
        int result = author1 != null ? author1.hashCode() : 0;
        result = 31 * result + (author2 != null ? author2.hashCode() : 0);
        return result;
    }
}
