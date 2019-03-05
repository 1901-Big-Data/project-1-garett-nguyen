
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;



public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	/*
	 * The map method runs once for each line of text in the input file.
	 * The method receives a key of type LongWritable, a value of type
	 * Text, and a Context object.
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
	throws IOException, InterruptedException {

	//deletes inline commas so that the string values can be parsed through csv
		
	String rawline = value.toString();
	String line = rawline.replace(", ", " ");
	
	String[] data = line.split(",");
	
	//looking for the employment to population ratio, according to ILO data.
	if (data[2].contains("Employment to population ratio") && !data[2].contains("24") && data[2].contains("ILO")) {
		try {
			double percentin2X = -1;
			double percentlatest = -1;
			String temp; 
			
			//find the earliest percentage of employment, starting from year 2000. if not 2000, then 2001 and so on.
			if (!data[44].equals("\"\"")) {
				temp = data[44].substring(1, data[44].length()-1);
				percentin2X = Double.parseDouble(temp);
			}
			else {
				for (int i = 45; i < data.length; i++) {
					if (!data[44].equals("\"\"")) {
						temp = data[44].substring(1, data[44].length()-1);
						percentin2X = Double.parseDouble(temp);
					}
				}
			}
			
			//find the latest percentage or employment. if not 2016, 2015 and so on. 
			if (!data[data.length - 1].equals("\"\"")) {
				temp = data[data.length-1].substring(1, data[data.length-1].length()-1);
				percentlatest = Double.parseDouble(temp);
			}
			else {
				for (int i = data.length-2; i > data.length-16; i--) {
					if (!data[i].equals("\"\"")) {
						temp = data[i].substring(1, data[i].length()-1);
						percentlatest = Double.parseDouble(temp);
					}
				}
			}
		
			if (percentlatest != -1 && percentin2X != -1 && percentin2X != 0) {
				
				double percentdiff = ((percentlatest/percentin2X)-1) * 100;
				
				if(data[2].contains("female")) {
					context.write(new Text(data[0].substring(0, data[0].length() - 1) + " female\""), new IntWritable((int) percentdiff));
				}
				else if (data[2].contains("male")) {
					context.write(new Text(data[0].substring(0, data[0].length() - 1) + " male\""), new IntWritable((int) percentdiff));
				}
			}			
		}
		catch (Exception e) {
			
		}
	}
	
		
	}
}