
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

		
	String rawline = value.toString();
	String line = rawline.replace(", ", " ");
	
	String[] data = line.split(",");

	//finds the row of data that specifies target data: The percent of graduating women, cumulative, in the country
	if (data[2].equals("\"Educational attainment at least Bachelor's or equivalent population 25+ female (%) (cumulative)\"")) {
		
		//find maximum percentage from 1960 to present.
		//find year where maximum percentage was attained through counter variable
		
		double max = -1;
		int counter = 0;
		int year = 0;
		for (int i = 4; i < data.length; i++) {
			
			
			if (!data[i].equals("\"\"")) {
				String temp = data[i].substring(1, data[i].length()-1);
				try {
					if (max < Double.parseDouble(temp)) {
						max = Double.parseDouble(temp);
						year = 1960 + counter;
					}
				}
				catch (Exception e) {
					
				}
			}
			counter++;
		}
		
		//checks if maximum graduation percentage over ALL years is less than 30%. 
		//if so, writes country key and graduation percent value to context
		if (max < 30 && max >= 0) {
			context.write(new Text(data[0]), new IntWritable((int) max));
			context.write(new Text(data[0]), new IntWritable(year));
		}
			
	}
	else { //If not the selected string for column 3, return. aka skip it. 
		return; 
	}

		
}
}