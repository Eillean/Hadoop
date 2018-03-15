package com.transwarp.mysql2mysql;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

public class DBRecord implements Writable, DBWritable{
	private int id;
	private String title;
	private String content;
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	@Override
	public void readFields(ResultSet set) throws SQLException {
		this.id = set.getInt("id");
		this.title = set.getString("title");
		this.content = set.getString("content");
	}

	@Override
	public void write(PreparedStatement pst) throws SQLException {
		pst.setInt(1, id);
		pst.setString(2, title);
		pst.setString(3, content);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.id = in.readInt();
		this.title = Text.readString(in);
		this.content = Text.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.id);
		Text.writeString(out, this.title);
		Text.writeString(out, this.content);
	}

	@Override
	public String toString() {
		 return this.id + " " + this.title + " " + this.content;  
	}
}

