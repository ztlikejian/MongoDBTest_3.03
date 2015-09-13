package com.likejian.mongodb;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.csvreader.CsvWriter;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class Export2Csv {

	private static MongoClient mongoClient = null;
	private static final String host = "127.0.0.1";  
	private static final int port = 27017;  
	private static final String databaseName = "test";  
	private static MongoDatabase mongoDatabase;  
	private static MongoCollection<Document> userConnection = null;
	private static String collectionName = "user";
	
	static {  
		mongoClient = new MongoClient(host, port);
		mongoDatabase = mongoClient.getDatabase(databaseName);
		userConnection = mongoDatabase.getCollection(collectionName);
	}
	
	public static void main(String[] args) throws Exception {
		CsvUtil csvUtil = new CsvUtil();
		
		BasicDBObject showKey = new BasicDBObject("_id", 0);    //不显示的字段
		BasicDBObject orderKey = new BasicDBObject("pId", -1);  //排序字段
		String showKeyString[] = {"age", "name", "id", "pId"};	//要显示的字段
		for(String str : showKeyString){
			showKey.append(str, 1);
		}
		
		//FindIterable<Document> documents = userConnection.find().skip(0).limit(10).sort(orderKey);
		FindIterable<Document> documents = userConnection.find();
		
		List<List<String>> list = new ArrayList<List<String>>();
		List<String> contentList = null;
		String[] contents = new String[5];
		int i = 0;
		String csvFilePath = "D:/test.csv";
		CsvWriter wr = new CsvWriter(csvFilePath, ',', Charset.forName("GBK"));
		try {
			for(Document document : documents){
				contentList = new ArrayList<String>();
				contents = new String[showKeyString.length];
				i = 0;
				for(String key : showKeyString){
					contentList.add(document.get(key).toString());
					contents[i] = document.get(key).toString();
					i ++;
				}
				list.add(contentList);
				wr.writeRecord(contents);
			}
		}catch (Exception e) {
			e.printStackTrace();
		} finally {
			wr.close();
		}
	}

}
