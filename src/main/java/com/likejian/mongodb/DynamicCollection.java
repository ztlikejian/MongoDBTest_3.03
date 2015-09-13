package com.likejian.mongodb;

import java.util.ArrayList;
import java.util.List;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class DynamicCollection {

	private static Mongo mongo = null;
	private static DB db = null;
	private DBCollection dynamicCollection =  null;
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		//System.out.println((int)Math.ceil(2000/1000d));
		//System.out.println((int)Math.ceil(2008/1000d));
		mongo = new Mongo();
		db = mongo.getDB("test");
		CsvUtil csvUtil = new CsvUtil();
		System.out.println("读取文件开始");long time1 = System.currentTimeMillis();
		List<String[]> csvList = csvUtil.readCsv("D:\\test2.csv");
		System.out.println("读取文件结束");long time2 = System.currentTimeMillis();
		System.out.println("读取文件耗时：" + (time2 - time1) + "ms");
		System.out.println("文件总条数：" + csvList.size());
		List<DBObject> dbObjectList = null;
		DBObject user = null;
		DynamicCollection dynamicCollTest = new DynamicCollection();
		System.out.println("插入开始");long time3 = System.currentTimeMillis();
		int listCount = csvList.size();
		int loopCount = (int)Math.ceil(listCount/1000d);
		for(int k = 0; k < loopCount; k ++){
			dbObjectList = new ArrayList<DBObject>(); 
			for (int i = k * 1000; i < (k == loopCount-1 ? listCount : k * 1000 + 1000); i++) {
				user = new BasicDBObject();
				for(int j = 0; j < csvList.get(i).length; j++){
					user.put("col_" + j, csvList.get(i)[j]);
				}
				dbObjectList.add(user);
			}
			dynamicCollTest.dynamicInsert("collection201306", dbObjectList);
		}
		System.out.println("插入结束");long time4 = System.currentTimeMillis();
		System.out.println("插入耗时：" + (time4 - time3) + "ms");
	}

	public void dynamicInsert(String dynamicCollName, List<DBObject> dbObjectList){
		dynamicCollection = DynamicCollection.collectionIsExists(dynamicCollName);
		if(dynamicCollection != null){
			for(DBObject dbObject : dbObjectList){
				dynamicCollection.save(dbObject).getN();
			}
		}
	}
	/**
	 * 判断collection是否存在
	 */
	private static DBCollection collectionIsExists(String dynamicCollName){
		if (!db.collectionExists(dynamicCollName)) {
	        DBObject options = new BasicDBObject();
/*	        options.put("size", 20);
	        options.put("capped", 20);
	        options.put("max", 20);*/
	        return db.createCollection(dynamicCollName, options);
	    }
		return db.getCollection(dynamicCollName);
	}
}
