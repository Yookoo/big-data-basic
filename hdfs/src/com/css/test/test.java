package com.css.test;


import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class test {

	@Test
	public void test() throws Exception {
		/*
		 * 通过url读取百度
		 */
		
//		URL url = new URL("http://www.baidu.com");
//		InputStream inputStream = url.openStream();
//		IOUtils.copyBytes(inputStream, System.out, 4096 ,true);
		
		/*
		 *通过url读取hdfs文件 
		 */
//		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
//		URL url = new URL("hdfs://192.168.56.100:9000/text.data");
//		InputStream inputStream = url.openStream();	
//		IOUtils.copyBytes(inputStream, System.out, 4096 ,true);
		
		/*
		 * 使用FileSystem类
		 */
		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS", "hdfs://192.168.56.100:9000");
		FileSystem fileSystem = FileSystem.get(configuration);
		
//		boolean b1 = fileSystem.exists(new Path("/text.data"));
//		System.out.println("exists:"+b1);
//		
//		boolean b2 = fileSystem.mkdirs(new Path("/test"));
//		System.out.println("mkdirs:"+b2);
//		
//		boolean b3 = fileSystem.exists(new Path("/test"));
//		System.out.println("exists:"+b3);
//		
//		boolean b4 = fileSystem.delete(new Path("/test"),true);
//		System.out.println("delete:"+b4);
//		
//		boolean b5 = fileSystem.exists(new Path("/test"));
//		System.out.println("exists:"+b5);
		
		FSDataOutputStream outputStream = fileSystem.create(new Path("/test/test2.data"),true);
		
		FileInputStream inputStream = new FileInputStream("D://README.txt");
//		IOUtils.copyBytes(inputStream, outputStream, configuration);

		
		//byte 不是 Byte		
		byte[] buffer = new byte[4096]; 
		int len = inputStream.read(buffer);
		if(len != -1){
			outputStream.write(buffer,0 ,len);
			len = inputStream.read(buffer);
		}
		inputStream.close();
		outputStream.close();
		
	
	}
}
