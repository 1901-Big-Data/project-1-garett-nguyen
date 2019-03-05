import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class CompositeWritable implements Writable {
	private int maxPercent = 0; 
	private int yearOfMaxPerc = 0;
	
	public CompositeWritable(){}
	
	public CompositeWritable(int val1, int val2) {
		maxPercent = val1;
		yearOfMaxPerc = val2;
	}
	
	public void readFields(DataInput in) throws IOException {
		maxPercent = in.readInt();
		yearOfMaxPerc = in.readInt();
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(maxPercent);
		out.writeInt(yearOfMaxPerc);
	}
	public void merge(CompositeWritable other) {
		maxPercent += other.getPercent();
		yearOfMaxPerc += other.getYear();
	}
	public int getPercent() {
		return maxPercent;
	}
	public int getYear() {
		return yearOfMaxPerc;
	}
	public void setPercent(int val) {
		maxPercent = val;
	}
	public void setYear(int val) {
		yearOfMaxPerc = val;
	}
	public String toString() {
		return this.maxPercent + "\t" + this.yearOfMaxPerc;
	}
}
