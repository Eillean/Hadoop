package com.transwarp.mysql2mysql;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class DBRecordReduce extends MapReduceBase implements Reducer<LongWritable, Text, DBRecord, Text> {
	@Override
	public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<DBRecord, Text> output,
			Reporter reporter) throws IOException {
		String[] splits = values.next().toString().split(" ");
		DBRecord r = new DBRecord();
		r.setId(Integer.parseInt(splits[0])); 
		r.setTitle(splits[1]);
		r.setContent(splits[2]);
		
		output.collect(r, new Text(r.getContent()));
	}
}
