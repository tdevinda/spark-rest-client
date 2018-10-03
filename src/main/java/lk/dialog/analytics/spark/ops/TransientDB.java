package lk.dialog.analytics.spark.ops;

import com.google.gson.JsonElement;
import lk.dialog.analytics.spark.models.JobResponse;
import org.bson.Document;

import java.util.List;

public interface TransientDB {

    public void storeData(Integer id, JobResponse data);

    public JobResponse getAllData(Integer id);

    public void addData(Integer id, List<Document> data);

    public JobResponse getData(Integer id, Integer from, Integer to);

    public Integer getHitCount(Integer id);

    public void removeData(Integer id);


}
