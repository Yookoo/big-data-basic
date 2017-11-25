package com.css.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * 
 * hdfs�����ࡢ
 * @author ZHU
 *
 */
public class HDFSUtils {
//	private String name = "fs.defaultFS";
//	private String url = "hdfs://192.168.56.100:9000";

	private FileSystem fileSystem;

	public HDFSUtils(String name,String url) throws IOException {
		Configuration configuration = new Configuration();
		configuration.set(name, url);
		fileSystem = FileSystem.get(configuration);
	}

	/*
	 * �г�ָ��Ŀ¼�µ����к��ӽڵ�
	 */
	public FileStatus[] list(String filename) throws RuntimeException, IOException{
		Path path = new Path(filename);
		if(fileSystem.exists(path)){
			return fileSystem.listStatus(path);
		}else {
			throw new RuntimeException("�������Ŀ¼������");
		}
	}
	/*
	 * ��ӡ���ı��ļ�������
	 */
	public void text(String filename) throws RuntimeException, IOException{
		Path path = new Path(filename);
		if(fileSystem.exists(path)){
			FSDataInputStream inputStream = fileSystem.open(path);
			IOUtils.copyBytes(inputStream, System.out, 4096 ,true);
		}else {
			throw new RuntimeException("��������ļ�������");
		}
	}
	/*
	 * ���䱾���ļ���HDFS
	 */
	public void put(String src,String dest) throws IOException{
		FSDataOutputStream outputStream = fileSystem.create(new Path(dest),true);
		FileInputStream inputStream = new FileInputStream(src);
		int available = inputStream.available();
		byte[] buffer = new byte[4096]; 
		int len = inputStream.read(buffer);
		int i = 1;
		while(len != -1){		
			outputStream.write(buffer,0 ,len);
			double rate =((i*409600.0)/available);
			DecimalFormat df = new DecimalFormat("#.0");
			System.out.println("���ϴ���"+df.format(rate)+"%" );
			len = inputStream.read(buffer);
			i=i+1;
		}
		inputStream.close();
		outputStream.close();
	}
	/*
	 * ɾ��HDFS�ļ�
	 */
	public boolean delete(String filename) throws RuntimeException, IOException{
		Path path = new Path(filename);
		if(fileSystem.exists(path)){
			return fileSystem.delete(path, true);
		}else {
			throw new RuntimeException("��������ļ�·�������ڣ�");
		}
	}
	/*
	 * �½�HDFSĿ¼
	 */
	public boolean makeDirs(String filename) throws RuntimeException, IOException{
		Path path = new Path(filename);
		if(fileSystem.exists(path)){
			throw new RuntimeException("��������ļ�·���Ѵ��ڣ�");
		}else {
			return fileSystem.mkdirs(path);
		}
	}
}
