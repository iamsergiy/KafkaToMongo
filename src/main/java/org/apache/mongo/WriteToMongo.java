package org.apache.mongo;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;

import static com.mongodb.client.model.Filters.eq;

/**
 * Custom connector of MongoDB for Flink Streaming
 *
 * Clase conectora que sirve para escribir los datos procesados de Kafka en MongoDB
 *
 * @author sergiy.shvayka
 */
public class WriteToMongo extends RichSinkFunction<Tuple2<String,Integer>>{

    private String host;
    private int port;
    private String name_db;
    private String collName;
    private Operations op;

    public WriteToMongo(String host, int port, String name_db, String collName){
        this.host = host;
        this.port = port;
        this.name_db = name_db;
        this.collName = collName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.op = new Operations(host, port, name_db);
    }

    public void close(){
        op.closeConnection();
    }

    @Override
    public void invoke(Tuple2<String,Integer> record) throws Exception {
        Document doc;
        MongoCollection<Document> coll = op.getDb().getCollection(collName);

        FindIterable<Document> found = coll.find(eq("name", record.getField(0)));
        if (found.first() == null) {
            doc = new Document("name", record.getField(0)).append("counts", 1);

        } else {
            int counts = found.first().getInteger("counts");
            doc = new Document("name", record.getField(0)).append("counts", counts + 1);
            coll.deleteMany(eq("name", record.getField(0)));
        }
        op.createDocument(doc, collName);

        System.out.println("Se ha insertado el registro \"" + doc + "\"");
    }
}