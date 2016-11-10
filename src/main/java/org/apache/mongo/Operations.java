package org.apache.mongo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.List;

/**
 * Utils class that provides basic tools for managing connection and insertion of MongoDB
 *
 * Clase auxiliar que contiene funciones que sirven para conectar y gestionar los datos en MongoDB
 *
 * @author sergiy.shvayka
 */
public class Operations {

    private MongoDatabase db;
    private MongoClient mongoClient;

    public Operations(String host, int port, String dbName){
        this.db = connectToDB(host, port, dbName);
    }

    private MongoDatabase connectToDB(String host, int port, String dbName){
        mongoClient = new MongoClient(host, port);
        MongoDatabase db = mongoClient.getDatabase(dbName);
        return db;
    }

    public void createDocument(Document doc, String collName){
        try{
            MongoCollection<Document> coll = db.getCollection(collName);
            coll.insertOne(doc);
        } catch (Exception e){}
    }

    public void closeConnection(){
        mongoClient.close();
    }

    public MongoDatabase getDb() {
        return db;
    }
}
