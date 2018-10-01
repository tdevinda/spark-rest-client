package lk.dialog.analytics.spark.ops;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;

public class SparkConnection {

    private Connection connection;
    private Logger logger;

    public SparkConnection(String database) throws SQLException, ClassNotFoundException {
        AppProperties properties = AppProperties.getInstance();

        Class.forName("org.apache.hive.jdbc.HiveDriver");
        connection = DriverManager.getConnection(
                String.format("jdbc:hive2://%s:%d/%s", properties.getIpAddress(), properties.getPort(), database),
                properties.getUsername(),
                properties.getPassword());


        logger = LogManager.getLogger(getClass());


    }


    public JsonElement execute(String query) {
        JsonArray result = new JsonArray();
        try {
            Statement statement = connection.createStatement();
            logger.debug(String.format("Starting execution for query '%s...'", query.substring(0, 30)));
            ResultSet rs = statement.executeQuery(query);
            logger.debug(String.format("Completed execution for query '%s...'", query.substring(0, 30)));
            ResultSetMetaData metaData = rs.getMetaData();
            String[] columns = new String[metaData.getColumnCount()];
            for (int i = 1; i <= columns.length; i++) {
                columns[i - 1] = metaData.getColumnName(i);
            }
            logger.debug(String.format("%d cols in response", columns.length));
            while (rs.next()) {
                JsonObject item = new JsonObject();
                for (String col : columns) {
                    item.addProperty(col, rs.getString(col));
                }
                result.add(item);

            }

            return result;
        } catch (SQLException e) {
            logger.warn(String.format("Error occured while running query '%s...'", query.substring(0, 30)));
            e.printStackTrace();
        }

        return null;

    }
}
