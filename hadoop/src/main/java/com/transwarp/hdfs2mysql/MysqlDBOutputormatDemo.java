package com.transwarp.hdfs2mysql;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@SuppressWarnings({ "unused", "deprecation" })
public class MysqlDBOutputormatDemo extends Configured implements Tool {
	/**
	 * 实现DBWritable
	 * 
	 * TblsWritable需要向mysql中写入数据
	 */
	public static class TblsWritable implements Writable, DBWritable {
		String tbl_name;
		int tbl_age;

		public TblsWritable() {
		}

		public TblsWritable(String name, int age) {
			this.tbl_name = name;
			this.tbl_age = age;
		}

		@Override
		public void write(PreparedStatement statement) throws SQLException {
			statement.setString(1, this.tbl_name);
			statement.setInt(2, this.tbl_age);
		}

		@Override
		public void readFields(ResultSet resultSet) throws SQLException {
			this.tbl_name = resultSet.getString(1);
			this.tbl_age = resultSet.getInt(2);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(this.tbl_name);
			out.writeInt(this.tbl_age);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.tbl_name = in.readUTF();
			this.tbl_age = in.readInt();
		}

		public String toString() {
			return new String(this.tbl_name + " " + this.tbl_age);
		}
	}

	public static class StudentMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class StudentReducer extends Reducer<LongWritable, Text, TblsWritable, TblsWritable> {
		@Override
		protected void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// values只有一个值，因为key没有相同的
			StringBuilder value = new StringBuilder();
			for (Text text : values) {
				value.append(text);
			}

			String[] studentArr = value.toString().split("\t");

			if (StringUtils.isNotBlank(studentArr[0])) {
				/*
				 * 姓名 年龄（中间以tab分割） 张明明 45
				 */
				String name = studentArr[0].trim();

				int age = 0;
				try {
					age = Integer.parseInt(studentArr[1].trim());
				} catch (NumberFormatException e) {
				}

				context.write(new TblsWritable(name, age), null);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		// 数据输入路径和输出路径
		String[] args0 = { "hdfs://192.168.21.30:9000/tmp/student.txt" };
		int ec = ToolRunner.run(new Configuration(), new MysqlDBOutputormatDemo(), args0);
		System.exit(ec);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// 读取配置文件
		Configuration conf = new Configuration();

		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://192.168.21.30:3306/test", "root",
				"root");

		// 新建一个任务
		Job job = new Job(conf, "DBOutputormatDemo");
		// 设置主类
		job.setJarByClass(MysqlDBOutputormatDemo.class);

		// 输入路径
		FileInputFormat.addInputPath(job, new Path(arg0[0]));

		// Mapper
		job.setMapperClass(StudentMapper.class);
		// Reducer
		job.setReducerClass(StudentReducer.class);

		// mapper输出格式
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		// 输入格式，默认就是TextInputFormat
		// job.setInputFormatClass(TextInputFormat.class);
		// 输出格式
		job.setOutputFormatClass(DBOutputFormat.class);

		// 输出到哪些表、字段
		DBOutputFormat.setOutput(job, "student", "name", "age");

		// 添加mysql数据库jar
		// job.addArchiveToClassPath(new
		// Path("hdfs://ljc:9000/lib/mysql/mysql-connector-java-5.1.31.jar"));
		// DistributedCache.addFileToClassPath(new
		// Path("hdfs://ljc:9000/lib/mysql/mysql-connector-java-5.1.31.jar"),
		// conf);
		// 提交任务
		return job.waitForCompletion(true) ? 0 : 1;
	}
}