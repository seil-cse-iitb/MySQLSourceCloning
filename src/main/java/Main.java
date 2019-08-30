import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;

import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;

import static org.apache.spark.sql.functions.col;
// mysql -u root -p -P 3307 -h seil.cse.iitb.ac.in

public class Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SourceMySQLCloning");
        conf.setIfMissing("spark.master", "local[4]");
        sparkLogsOff();

        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        Properties sourceProperties = new Properties();
        sourceProperties.setProperty("user", "reader");
        sourceProperties.setProperty("password", "reader");
        sourceProperties.setProperty("driver", "com.mysql.cj.jdbc.Driver");

        Properties targetProperties = new Properties();
        targetProperties.setProperty("user", "root");
        targetProperties.setProperty("password", "MySQL@seil");
        targetProperties.setProperty("driver", "com.mysql.cj.jdbc.Driver");

        String sourceURL = "jdbc:mysql://mysql.seil.cse.iitb.ac.in:3306/seil_sensor_data?useSSL=false&autoReconnect=true&failOverReadOnly=false&maxReconnects=10&useUnicode=true&characterEncoding=UTF-8";
        String targetURL = "jdbc:mysql://mysql.seil.cse.iitb.ac.in:3307/seil_sensor_data?useSSL=false&autoReconnect=true&failOverReadOnly=false&maxReconnects=10&useUnicode=true&characterEncoding=UTF-8";
//        String targetURL = "jdbc:mysql://10.129.149.19:3306/seil_sensor_data_source_3?useSSL=false&autoReconnect=true&failOverReadOnly=false&maxReconnects=10&useUnicode=true&characterEncoding=UTF-8";

        long startTime = 1514745000;//2017-01-01
        long endTime = 1546626600;//2018-01-01
        int numPartitions = 1000;
        System.out.println("Coping data started from " + new Date(startTime * 1000) + " to " + new Date(endTime * 1000));
        Dataset<Row> sd = sparkSession.read()
                .jdbc(sourceURL, "sch_3", "ts", startTime, endTime,
                        numPartitions, sourceProperties);
        Column tsFilter = col("TS").$greater$eq(startTime).and(col("TS").$less(endTime));
        Column sensorFilter = col( "sensor_id")//.equalTo("power_k_m")
//                .or(col("sensor_id").equalTo("power_k_sr_a"))
//                .or(col("sensor_id").equalTo("power_k_erts_a"))
//                .or(col("sensor_id").equalTo("power_k_erts_l"))
//                .or(col("sensor_id").equalTo("power_k_erts_p"))
                //.or(col("sensor_id")
                        .equalTo("power_k_seil_a")//)
                .or(col("sensor_id").equalTo("power_k_seil_l"))
                .or(col("sensor_id").equalTo("power_k_seil_p"));
        sd = sd.filter(tsFilter).filter(sensorFilter);
        sd.write().mode(SaveMode.Append).jdbc(targetURL,"sch_3",targetProperties);
//        sd = sd.withColumn("TS", col("TS").cast(DataTypes.LongType).minus(89424000));
//        sd.cache();

//        for (int day = -5+365+360; day <=365+365+365+5; day++) {
////            sd.select(col("TS").cast(DataTypes.TimestampType), col("sensor_id")).show(false);
//            System.out.println("Day: "+day);
////            sd.withColumn("TS", col("TS").cast(DataTypes.LongType).plus(day*86400))
////            .write().mode(SaveMode.Append).jdbc(targetURL, "sch_3", targetProperties);
//        }
//        sd.unpersist();
    }

    public static void main1(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SourceMySQLCloning");
        conf.setIfMissing("spark.master", "local[4]");
        sparkLogsOff();

        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        Properties sourceProperties = new Properties();
        sourceProperties.setProperty("user", "reader");
        sourceProperties.setProperty("password", "reader");
        sourceProperties.setProperty("driver", "com.mysql.cj.jdbc.Driver");

        Properties targetProperties = new Properties();
        targetProperties.setProperty("user", "root");
        targetProperties.setProperty("password", "MySQL@seil");
        targetProperties.setProperty("driver", "com.mysql.cj.jdbc.Driver");

        String sourceURL = "jdbc:mysql://mysql.seil.cse.iitb.ac.in:3306/seil_sensor_data?useSSL=false&autoReconnect=true&failOverReadOnly=false&maxReconnects=10&useUnicode=true&characterEncoding=UTF-8";
        String targetURL = "jdbc:mysql://seil.cse.iitb.ac.in:3307/seil_sensor_data?useSSL=false&autoReconnect=true&failOverReadOnly=false&maxReconnects=10&useUnicode=true&characterEncoding=UTF-8";

        long startTime = 1511084255;//2017-11-19 09:37:35
        long endTime = 1546281000;
        int numPartitions = 12;
        long interval = 24 * 60 * 60;//1 day
        for (long tstart = startTime; tstart < endTime; tstart += interval) {
            System.out.println("Coping data started from " + new Date(tstart * 1000) + " to " + new Date((tstart + interval) * 1000));
            Dataset<Row> sourceDataset = sparkSession.read()
                    .jdbc(sourceURL, "sch_3", "ts", tstart, tstart + interval,
                            numPartitions, sourceProperties);
            Column tsFilter = col("TS").$greater$eq(tstart).and(col("TS").$less(tstart + interval));
//            Column sensorFilter = col("sensor_id").equalTo("power_k_seil_p");
            sourceDataset = sourceDataset.filter(tsFilter);
            sourceDataset.write().mode(SaveMode.Append).jdbc(targetURL, "sch_3", targetProperties);
            sourceDataset.unpersist();
        }
    }

    public static void sparkLogsOff() {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
    }
}
