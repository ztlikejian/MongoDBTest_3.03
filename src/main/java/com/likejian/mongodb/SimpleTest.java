package com.likejian.mongodb;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;

public class SimpleTest {

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
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// 查询所有的Database
		for (String name : mongoClient.listDatabaseNames()) {
			print("dbName: " + name);
		}
		// 查询所有的集合
		for (Document name : mongoDatabase.listCollections()) {
			print("collectionName: " + name);
		}
		if (mongoDatabase.getCollection(collectionName) == null) {
			mongoDatabase.createCollection(collectionName);
		}
		userConnection = mongoDatabase.getCollection(collectionName);
		userConnection.drop();
		//save_1();
		//save_2();
		queryCount();
		//queryAll();
		//remove("55f4ed5346589f18142a3a26");
		//queryCount();
		//modify();
	}

	private static void save_1() {
		Document user = new Document();
		user.put("name", "likejian");
		user.put("age", 27);
		user.put("id", 1000);
		user.put("pId", 10000);
		userConnection.insertOne(user);
	}

	private static void save_2() {
		// 插入 List集合
		List<Document> documents = new ArrayList<Document>();
		for (int i = 0; i < 1000000; i++) {
			Document user = new Document();
			user.put("name", "likejian_" + i);
			user.put("age", 24);
			user.put("id", 1000 + i);
			user.put("pId", 10000 + i);
			documents.add(user);
		}
		userConnection.insertMany(documents);
	}

	/**
	 * 查询条数
	 */
	private static void queryCount() {
		print(userConnection.count());
	}

	/**
	 * 查找所有数据
	 */
	private static void queryAll() {
		FindIterable<Document> iterable = userConnection.find();
		MongoCursor<Document> cursor = iterable.iterator();
		while (cursor.hasNext()) {
			Document user = cursor.next();
			print(user.toString());
		}
	}

	/**
	 * 删除数据
	 */
	private static void remove(String id) {
		BasicDBObject filter = new BasicDBObject("_id", new ObjectId(id));
		print(userConnection.deleteOne(filter).getDeletedCount());
	}

	/**
	 * 修改数据
	 */
	private static void modify() {
		BasicDBObject filter = null;
		filter = new BasicDBObject("_id", new ObjectId("55f4ed8146589f333c2c3368"));
		BasicDBObject newContent = new BasicDBObject();
		newContent.put("name", "李克俭");
		newContent.put("age", 18);
		BasicDBObject update = new BasicDBObject("$set",newContent); 
		print(userConnection.updateOne(filter, update).getModifiedCount());
		
		
		filter = new BasicDBObject("name", "likeqin");
		Document replacement = new Document();
		replacement.put("name", "likeqin");
		replacement.put("age", 24);
		replacement.put("id", 1001);
		replacement.put("pId", 10001);
		BasicDBObject update2 = new BasicDBObject("$set", replacement); 
		UpdateOptions updateOptions = new UpdateOptions().upsert(true);		//true:如果数据库不存在，则添加	
		print(userConnection.updateOne(filter, update2, updateOptions).getModifiedCount());
	}

	private static void print(Object str) {
		System.out.println(str);
	}
	
	/** 
     * 将实体类的obj的字段信息和内容动态放到mapParams里面 
     *  
     * @param mapParams 
     * @param obj 
     * @param method 
     */  
    public static void dymParms(Object mapParams, Object obj, String method) {  
        try {  
            if (obj != null) {  
                Field[] fields = obj.getClass().getDeclaredFields();  
                Class<?>[] arrClazz = new Class[2];  
                arrClazz[0] = String.class;  
                arrClazz[1] = Object.class;  
                Method m = mapParams.getClass().getDeclaredMethod(method,  
                        arrClazz);  
                m.setAccessible(true);  
                if (fields != null) {  
                    for (Field f : fields) {  
                        f.setAccessible(true);  
                        Object value = f.get(obj);  
                        if (null!=value) {  
                            Class<?> clazz = value.getClass();  
                            Object[] strs = new Object[2];  
                            if (clazz == String.class) {  
                                if ( !"".equals(String.valueOf(value))) {  
                                    strs[0] = f.getName();  
                                    strs[1] = value;  
                                    m.invoke(mapParams, strs);  
                                }  
                            } else {  
                                strs[0] = f.getName();  
                                strs[1] = value;  
                                m.invoke(mapParams, strs);  
                            }  
                        }  
                    }  
                }  
            }  
        } catch (SecurityException e) {  
            e.printStackTrace();  
        } catch (IllegalArgumentException e) {  
            e.printStackTrace();  
        } catch (IllegalAccessException e) {  
            e.printStackTrace();  
        } catch (Exception e) {  
            e.printStackTrace();  
        }  
    }  
}
