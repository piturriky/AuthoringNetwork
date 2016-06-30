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
        boolean crossEquals = author1.equals(o.author2) && author2.equals(o.author1);

        return crossEquals ? 0 : author1.compareTo(o.author1) + author2.compareTo(o.author2);
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

        return (author1.equals(that.author1) && author2.equals(that.author2)) ||
                (author1.equals(that.author2) && author2.equals(that.author1));

    }

    @Override
    public int hashCode() {
        return author1.hashCode() + author2.hashCode();
    }
}
