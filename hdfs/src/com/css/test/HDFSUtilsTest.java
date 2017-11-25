package com.css.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.css.utils.HDFSUtils;

public class HDFSUtilsTest {
	private HDFSUtils hdfs;
	
	@Before
	public void init() throws IOException{

		 hdfs = new HDFSUtils("fs.defaultFS", "hdfs://192.168.56.100:9000");
	
	}
	@Test
	public void listTest() throws RuntimeException, IOException{
		FileStatus[] status = hdfs.list("/test");
		Assert.assertEquals(2, status.length);
	}
	
	@Test
	public void textTest() throws RuntimeException, IOException{
		hdfs.text("/test/test.data");
	}
	
	@Test
	public void putTest() throws RuntimeException, IOException{
		hdfs.put("D://12306.rar","/12306.rar");
	}
	
	@Test
	public void deleteTest() throws RuntimeException, IOException{
		hdfs.delete("/test2");
	}
	@Test
	public void makeDirsTest() throws RuntimeException, IOException{
		hdfs.makeDirs("/test2");
	}
}
