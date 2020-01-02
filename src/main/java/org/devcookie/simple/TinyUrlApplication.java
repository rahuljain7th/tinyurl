package org.devcookie.simple;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.util.SerializationUtils;

import javax.annotation.PostConstruct;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


@SpringBootApplication
public class TinyUrlApplication {
    private static final Logger logger = LoggerFactory.getLogger(TinyUrlApplication.class);
    private static final String RANGE_NAMESPACE = "/range";
    private static final int RANGEDIFF = 100;
    private static final int DEFAULT_RANGE = 0;
    private static Long MAX_LIMIT = 1000L;
    private CuratorFramework curatorFramework;
    private String currentZNodePath;

    public static void main(String[] args) {
        SpringApplication.run(TinyUrlApplication.class, args);
    }

    @PostConstruct
    private void init() {
        logger.info("TinyUrlApplication Application Start");
        registerInZookeeper(2181);
    }

    private void registerInZookeeper(int port) {
        try {
            this.curatorFramework = CuratorFrameworkFactory.newClient("localhost:2181",
                    new RetryNTimes(5, 1000));
            this.curatorFramework.start();
            if (this.curatorFramework.checkExists().forPath(RANGE_NAMESPACE) == null) {
                // Creating a Range List
                List<Long> rangeList = rangeListCreator();
                byte[] rangeBytes = SerializationUtils.serialize(rangeList);
                // creating the Parent zNode with Range and Storing List of Range.
                this.curatorFramework.create().forPath(RANGE_NAMESPACE, rangeBytes);
            }
            //TODO Handle Range Finish Scenario.
            // Once the App Starts.Pick up Range.
            createAppNode();
            // handle the case when range is finished.
        } catch (Exception e) {
            logger.error("Exception  in registerInZookeeper", e);
        }

    }

    private void createAppNode() throws Exception {
        Long rangeMin = pickRange();
        long[] rangePicked = new long[]{rangeMin, rangeMin + RANGEDIFF};
        logger.info("Range Picked for this APP {} - {}", rangeMin, rangeMin + RANGEDIFF);
        this.currentZNodePath = this.curatorFramework.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL
        ).forPath(RANGE_NAMESPACE + "/App_", toByte(rangePicked));
    }


    public Long getCurrentIndex() throws Exception {
        byte[] rangeIndexByte = this.curatorFramework.getData().forPath(this.currentZNodePath);
        long[] rangeIndex = ByteBuffer.wrap(rangeIndexByte).asLongBuffer().array();
        long currentIndex = rangeIndex[0];
        rangeIndex[0] = currentIndex + 1;
        if (currentIndex <= rangeIndex[1]) {
            this.curatorFramework.setData().forPath(RANGE_NAMESPACE, toByte(rangeIndex));
        } else {
            Long rangeMin = pickRange();
            long[] rangePicked = new long[]{rangeMin, rangeMin + RANGEDIFF};
            this.curatorFramework.setData().forPath(RANGE_NAMESPACE, toByte(rangePicked));
        }
        return currentIndex;
    }

    private Long pickRange() throws Exception {
        byte[] parentRangeDate = this.curatorFramework.getData().forPath(RANGE_NAMESPACE);
        List<Long> rangeList = (List<Long>) SerializationUtils.deserialize(parentRangeDate);
        logger.info("Range Available {}", rangeList);
        long range = rangeList.get(0);
        rangeList.remove(0);
        this.curatorFramework.setData().forPath(RANGE_NAMESPACE, SerializationUtils.serialize(rangeList));
        return range;
    }

    private static List<Long> rangeListCreator() {
        List<Long> ranges = new ArrayList<>();
        long noofRanges = MAX_LIMIT / RANGEDIFF;
        for (long i = 0; i < noofRanges; i++) {
            ranges.add(i * RANGEDIFF);
        }
        return ranges;
    }

    public static byte[] toByte(long[] longArray) {
        ByteBuffer bb = ByteBuffer.allocate(longArray.length * Long.BYTES);
        bb.asLongBuffer().put(longArray);
        return bb.array();
    }
}
