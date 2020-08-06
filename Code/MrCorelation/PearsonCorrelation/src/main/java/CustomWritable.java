import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class CustomWritable implements Writable {
    Integer count;
    Integer agi;
    Integer number;
    Double coefficient;

    public CustomWritable() {
        number = 0;
        coefficient = Double.valueOf(0);
    }

    public Double getCoefficient() {
        return coefficient;
    }

    public void setCoefficient(Double coefficient) {
        this.coefficient = coefficient;
    }

    public Integer getNumber() {
        return number;
    }

    public void setNumber(Integer number) {
        this.number = number;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public Integer getAgi() {
        return agi;
    }

    public void setAgi(Integer agi) {
        this.agi = agi;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(count);
        dataOutput.writeInt(agi);
        dataOutput.writeInt(number);
        dataOutput.writeDouble(coefficient);
    }

    public void readFields(DataInput dataInput) throws IOException {
        count = dataInput.readInt();
        agi = dataInput.readInt();
        number = dataInput.readInt();
        coefficient = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return "coefficient = " + coefficient;
    }
}