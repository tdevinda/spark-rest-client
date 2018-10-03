package lk.dialog.analytics.spark.ops;

import lk.dialog.analytics.spark.models.JobResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Executes and fetches query results for thrift connections made to the spark database
 */
public class QueryExecutor {

    private Map<String, SparkConnection> connectionMap;
    private Map<String, ThreadPoolExecutor> executorMap;

    private MongoTransientDB transientDB;


    private static final int STAGEWISE_TRIGGERRING_HITCOUNT = 200000;

    private Logger logger;
    public QueryExecutor() {
        connectionMap = new HashMap<>();
        executorMap = new HashMap<>();

        transientDB = MongoTransientDB.getInstance();
        logger = LogManager.getLogger(getClass());
    }


    /**
     * Submits a query to be executed in a selected spark connection. A connection will be
     *   created for each database name. Only one query is allowed per connection.
     * @param dbName
     * @param query
     * @param id Query id to fetch results later
     * @return
     */
    public boolean submit(String dbName, String query, Integer id) {

        //if the executor for this db is missing, create it.
        if (executorMap.get(dbName) == null) {
            executorMap.put(dbName, new ThreadPoolExecutor(1, 1,
                    AppProperties.getInstance().getExecTimeMin(), TimeUnit.MINUTES,
                    new ArrayBlockingQueue<>(AppProperties.getInstance().getQueueSize())));
        }

        //check whether the executor for this db is at full capacity
        if(executorMap.get(dbName).getQueue().remainingCapacity() == 0) {
            logger.info(String.format("Queue full for DB '%s'", id, dbName));
            return false;
        }


        //check if a connection is available in the map
        if (connectionMap.get(dbName) == null) {
            try {
                connectionMap.put(dbName, new SparkConnection(dbName));
            } catch (SQLException | ClassNotFoundException e) {
                logger.warn(String.format("Could not create a connection to DB '%s'", dbName));
                return false;
            }
        }

        //fetch the connection from the connections map and set the query running state to true
        SparkConnection connection = connectionMap.get(dbName);


        QueryThread thread = new QueryThread(id, query, connection, dbName);
        executorMap.get(dbName).submit(thread);
        logger.debug(String.format("Query submit for db: %s ID=%d", dbName, id));
        return true;
    }


    /**
     * Fetch the results of a submitted query as a JsonElement. If the result does not
     *   exist, null will be returned.
     * @param id
     * @param page
     * @return
     */
    public JobResponse getResult(Integer id, Integer page) {
        logger.debug("querying data for id=" + id);

        JobResponse data = null;
        int hits = transientDB.getHitCount(id);

        if (hits == -1) {
            logger.info("No hits for id=" + id);
            return null;
        }


        if (page == 0) {
            if (hits > STAGEWISE_TRIGGERRING_HITCOUNT) {
                logger.info(String.format("Going with stages for id=%d hits=%d", id, hits));
                data = transientDB.getData(id, 0, STAGEWISE_TRIGGERRING_HITCOUNT);
                data.setNext(String.format("status/%d/%d", id, page + 1));
            } else {
                data = transientDB.getAllData(id);
                if( data != null) {
                    transientDB.removeData(id);
                }
            }
        } else {
            logger.debug(String.format("getting page %d for id=%d", page, id));
            data = transientDB.getData(id,
                    page * STAGEWISE_TRIGGERRING_HITCOUNT,
                    STAGEWISE_TRIGGERRING_HITCOUNT);
            if (hits > (page + 1) * STAGEWISE_TRIGGERRING_HITCOUNT) {
                data.setNext(String.format("status/%d/%d", id, page + 1));
            } else {
                if (data != null) {
                    transientDB.removeData(id);
                }
            }
        }
        return data;
    }


    /**
     * Executes the actual query using the given connection.
     */
    private class QueryThread implements Runnable {

        private Integer id;
        private String query;
        private SparkConnection connection;

        public QueryThread(Integer id, String query, SparkConnection connection, String dbName) {
            this.id = id;
            this.query = query;
            this.connection = connection;
        }

        @Override
        public void run() {
            try {
                JobResponse response = new JobResponse();
                connection.setTransientDB(transientDB);

                boolean result = connection.executeWithTransientDb(id, query);
                response.setId(id);
                response.setSuccess(result);
                response.setTimedout(false);

                transientDB.addMetaInfo(response);
//                resultsMap.put(id, response);

                logger.debug("Stored data to db for id=" + id);
            } catch (Exception e) {
                System.out.println("we got killed");
                e.printStackTrace();
                JobResponse response = new JobResponse();
                response.setSuccess(true);
                response.setTimedout(false);
                transientDB.addMetaInfo(response);
            }
        }

        public Integer getId() {
            return id;
        }
    }



}
