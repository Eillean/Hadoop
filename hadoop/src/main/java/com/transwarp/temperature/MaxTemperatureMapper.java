package com.transwarp.temperature;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	private static final String MISSING = "9999.9";

	@Override
	public void map(LongWritable key, Text value, Context context) {
		String line = value.toString();
		String year = line.toString().substring(14, 18);
		String temp = "";

		if (line.toString().substring(24, 25).equals("-")) {
			temp = line.toString().substring(25, 30);
		} else if (line.toString().substring(25, 26).equals("-")) {
			temp = line.toString().substring(26, 30);
		} else {
			temp = line.toString().substring(26, 30);
		}

		if (!MISSING.equals(temp.trim()) && !"YEAR".equals(year.trim()) && !temp.trim().equals("TEMP")) {
			Double temperature = Double.parseDouble(temp.trim());
			try {
				context.write(new Text(year), new DoubleWritable(temperature));
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

}
