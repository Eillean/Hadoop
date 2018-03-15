package com.transwarp.mysql2mysql;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;

public class DBAccess {
	public static void main(String[] args) throws IOException {

		JobConf conf = new JobConf(DBAccess.class);
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setInputFormat(DBInputFormat.class);
		conf.setOutputFormat(DBOutputFormat.class);

		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://192.168.21.30:3306/test", "root",
				"root");
		String[] fields = { "id", "title", "content" };
		DBInputFormat.setInput(conf, DBRecord.class, "table1", null, "id", fields);

		// mapreduce 将数据输出到 table2 表
		DBOutputFormat.setOutput(conf, "table2", "id", "title", "content");

		conf.setMapperClass(DBRecordMapper.class);
		conf.setReducerClass(DBRecordReduce.class);

		JobClient.runJob(conf);

	}
}
