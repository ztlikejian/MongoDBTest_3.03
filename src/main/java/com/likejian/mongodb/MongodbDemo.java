package com.likejian.mongodb;
import java.util.HashMap;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
 
public class MongodbDemo {
 
    public static void main(String[] arsg) {
      
        MongoClient client = new MongoClient("127.0.0.1",27017);
        //MongoClient client = new MongoClient();
      
        MongoDatabase database  = client.getDatabase("testMongo");//Database name
      
        //MongoCollection<> collection = database.getCollection("");
          
        add(database);
        upate(database);
        delete(database);
        search(database);
        client.close();
    }
     
     
     
    public static void add(MongoDatabase database) {
        MongoCollection<Document> collection = database.getCollection("foo");//集合 name
         
        Map<String, String> map = new HashMap<String, String>();
        map.put("time", "2015-05-21 22:59");
         
        Document document = new Document("AddBy", "JAVA" ).append("append", map);
         
        collection.insertOne(document);
         
        database.createCollection("JavaCollection");
         
    }
     
    public static void delete(MongoDatabase database) {
        MongoCollection<Document> collection =  database.getCollection("foo");
        collection.deleteOne(Filters.lte("age", 22));
         
         database.getCollection("JavaCollection").drop();
    }
     
    public static void upate(MongoDatabase database){
        MongoCollection<Document> collection = database.getCollection("foo");
         
        //collection.updateOne(Filters.gt("age",18), new Document("$set", new Document("sex","MANNN")));
        collection.updateMany(Filters.gt("age",18), new Document("$set", new Document("sex","MANNN")));
    }
     
    public static void search(MongoDatabase database){
  
        MongoCollection<Document> collection = database.getCollection("foo");
         
        Bson bsonfilter = Filters.gte("age",22);
        FindIterable<Document> find = collection.find(bsonfilter);
         
        System.out.println("collection.count():: "+ collection.count());
         
        for(Document documentT : find) {
            System.out.println(documentT.get("sex"));
        }
          
        Document document = find.first();
        if(null !=document && document.containsKey("age")) {
          System.out.println(document.getDouble("age"));
        }
    }
}