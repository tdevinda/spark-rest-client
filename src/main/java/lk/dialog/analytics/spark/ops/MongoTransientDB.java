package lk.dialog.analytics.spark.ops;

import com.google.gson.Gson;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lk.dialog.analytics.spark.models.JobResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class MongoTransientDB implements TransientDB{
    private static MongoTransientDB ourInstance = new MongoTransientDB();

    public static MongoTransientDB getInstance() {
        return ourInstance;
    }

    private Logger logger;

    private MongoClient client;
    private MongoDatabase mongoDatabase;
    MongoCollection<Document> collection;

    private MongoTransientDB() {
        client = new MongoClient("localhost");
        mongoDatabase = client.getDatabase("polkichcha");
        collection = mongoDatabase.getCollection("pktrans");

        logger = LogManager.getLogger(getClass());
    }

    @Override
    public void storeData(Integer id, JobResponse data) {
        logger.debug("Starting to add data to db for id=" + id);
        Document doc = Document.parse(new Gson().toJson(data));
        doc.append("id", id);
        collection.insertOne(doc);
        logger.debug("Completed adding data to db for id=" + id);
    }

    @Override
    public JobResponse getAllData(Integer id) {
        Document doc = new Document("id", id);
        FindIterable<Document> result = collection.find(doc);
        String out = result.iterator().next().toJson();
        return new Gson().fromJson(out, JobResponse.class);
    }

    @Override
    public JobResponse getData(Integer id, Integer from, Integer to) {
        Document doc = new Document("id", id);
        Document slice = new Document();
        List<Integer> range = new ArrayList<>();
        range.add(from);
        range.add(to);
        slice.append("$slice", range);
        Document filter = new Document("data", slice);

        FindIterable<Document> results = collection.find(doc).projection(filter);
        if (results.iterator().hasNext()) {
            return null;
        } else {
            String out = results.iterator().next().toJson();
            return new Gson().fromJson(out, JobResponse.class);
        }
    }

    @Override
    public Integer getHitCount(Integer id) {
        List<Document> aggregations = new ArrayList<>();
        Document match = new Document("$match", new Document("id", id));

        Document project = new Document();
//        project.append("_id", 0);
        project.append("$project", new Document("len", new Document("$size", "$data")));

        aggregations.add(match);
        aggregations.add(project);

        AggregateIterable<Document> result = collection.aggregate(aggregations);
        if (result.iterator().hasNext()) {
            return result.iterator().next().getInteger("len");
        } else {
            return -1;
        }

    }

    @Override
    public void removeData(Integer id) {

    }
}
