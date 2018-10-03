package lk.dialog.analytics.spark.ops;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import lk.dialog.analytics.spark.models.JobResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.util.List;

public class MongoTransientDB{
    private static MongoTransientDB ourInstance = new MongoTransientDB();

    public static MongoTransientDB getInstance() {
        return ourInstance;
    }

    private Logger logger;

    private MongoClient client;
    private MongoDatabase mongoDatabase;
    MongoCollection<Document> collection;
    MongoCollection<Document> metaCollection;

    private MongoTransientDB() {
        client = new MongoClient(AppProperties.getInstance().getMongoLocation());
        mongoDatabase = client.getDatabase("polkichcha");
        collection = mongoDatabase.getCollection("pktrans");
        metaCollection = mongoDatabase.getCollection("pkmeta");
        logger = LogManager.getLogger(getClass());
    }

    public void storeData(Integer id, JobResponse data) {
        logger.debug("Starting to add data to db for id=" + id);
        Document doc = Document.parse(new Gson().toJson(data));
        doc.append("id", id);
        collection.insertOne(doc);
        logger.debug("Completed adding data to db for id=" + id);
    }

    public JobResponse getAllData(Integer id) {
        JobResponse response = new JobResponse();
        response.setId(id);

        Document metaDoc = new Document("id", id);
        FindIterable<Document> metaIterable = metaCollection.find(metaDoc);
        if (metaIterable.iterator().hasNext()) {
            logger.debug("Found metadata. looking for data");
            //there is data.
            Document projection = new Document();
            projection.put("id", 0);
            projection.put("_id", 0);
            FindIterable<Document> data = collection.find(metaDoc).projection(projection);

            response.setSuccess(true);
            response.setTimedout(false);

            JsonArray jsonArray = new JsonArray();
            JsonParser parser = new JsonParser();
            MongoCursor<Document> iterator = data.iterator();
            while (iterator.hasNext()) {
                Document record = iterator.next();
                jsonArray.add(parser.parse(record.toJson()));
            }
            response.setData(jsonArray);


        } else {
            response.setSuccess(false);
            response.setTimedout(false);
        }

        return response;


    }

    public void addData(Integer id, List<Document> data) {
        collection.insertMany(data);

    }

    public void addMetaInfo(JobResponse jobResponse) {
        Document document = new Document();
        document.put("success", jobResponse.isSuccess());
        document.put("timedout", jobResponse.isTimedout());
        document.put("id", jobResponse.getId());

        metaCollection.insertOne(document);
    }

    public JobResponse getData(Integer id, Integer from, Integer next) {
        JobResponse response = new JobResponse();
        response.setId(id);

        Document metaDoc = new Document("id", id);
        FindIterable<Document> metaIterable = metaCollection.find(metaDoc);
        if (metaIterable.iterator().hasNext()) {
            logger.debug("Found metadata. looking for data");
            //there is data.
            Document projection = new Document();
            projection.put("id", 0);
            projection.put("_id", 0);
            FindIterable<Document> data = collection.find(metaDoc).projection(projection).skip(from).limit(next);

            response.setSuccess(true);
            response.setTimedout(false);

            JsonArray jsonArray = new JsonArray();
            JsonParser parser = new JsonParser();
            MongoCursor<Document> iterator = data.iterator();
            while (iterator.hasNext()) {
                Document record = iterator.next();
                jsonArray.add(parser.parse(record.toJson()));
            }
            response.setData(jsonArray);


        } else {
            response.setSuccess(false);
            response.setTimedout(false);
        }

        return response;


    }

    public Integer getHitCount(Integer id) {
        Document countDocument = new Document("id", id);
        FindIterable<Document> findIterable = metaCollection.find(countDocument);
        if (findIterable.iterator().hasNext()) {
            long size = collection.count(countDocument);
            return (int) size;
        } else {
            return -1;
        }
    }

    public void removeData(Integer id) {

    }
}
