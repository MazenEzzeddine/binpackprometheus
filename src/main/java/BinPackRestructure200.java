import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BinPackRestructure200 {

    private static final Logger log = LogManager.getLogger(BinPackRestructure200.class);
    public static int size = 1;
    public Instant LastUpScaleDecision = Instant.now();
    static double wsla = 0.5;
    static List<Consumer> assignment = new ArrayList<Consumer>();
    static List<Consumer> currentAssignment = assignment;
    private static KafkaConsumer<byte[], byte[]> metadataConsumer;

    static List<Partition> partsReset;


    static {
        currentAssignment.add(new Consumer("0", (long) (200f * wsla * .9),
                200f * .9));
        for (Partition p : ArrivalRates.topicpartitions) {
            currentAssignment.get(0).assignPartition(p);
        }
    }


    public static void scaleAsPerBinPackRestructured() {
        log.info("Currently we have this number of consumers group {} {}", "testgroup1", size);
        if (assignmentViolatesTheSLA2()) {
            resetPartitions(0.9f);

            int neededsize = binPackAndScale();
            log.info("We currently need the following consumers for group1 (as per the bin pack) {}", neededsize);
            int replicasForscale = neededsize - size;
            if (replicasForscale > 0) {
                //TODO IF and Else IF can be in the same logic
                log.info("We have to upscale  group1 by {}", replicasForscale);
                try (final KubernetesClient k8s = new KubernetesClientBuilder().build()) {
                    k8s.apps().deployments().inNamespace("default").withName("latency").scale(neededsize);
                    log.info("I have Upscaled group {} you should have {}", "testgroup1", neededsize);
                }
                currentAssignment = assignment;
                size = neededsize;
                return;
            }
        } else {
            resetPartitions(0.4f);
            int neededsized = binPackAndScaled();
            int replicasForscaled = size - neededsized;
            if (replicasForscaled > 0) {
                log.info("We have to downscale  group by {} {}", "testgroup1", replicasForscaled);
                currentAssignment = assignment;
                size = neededsized;
                try (final KubernetesClient k8s = new KubernetesClientBuilder().build()) {
                    k8s.apps().deployments().inNamespace("default").withName("latency").scale(neededsized);
                    log.info("I have downscaled group {} you should have {}", "testgroup1", neededsized);
                }
            }
        }
        log.info("===================================");
    }


    private static boolean assignmentViolatesTheSLA2() {
        for (Consumer cons : currentAssignment) {
            double sumPartitionsArrival = 0;
            double sumPartitionsLag = 0;
            for (Partition p : cons.getAssignedPartitions()) {
                sumPartitionsArrival += ArrivalRates.topicpartitions.get(p.getId()).getArrivalRate();
                sumPartitionsLag += ArrivalRates.topicpartitions.get(p.getId()).getLag();
            }

            log.info("consumer co {}, lagsum {}, arrival sum {}", cons.getId(), sumPartitionsLag, sumPartitionsArrival);

            if (sumPartitionsLag >  (wsla * 200f * .9f) ||
                    sumPartitionsArrival > 200f * 0.9f) {
                log.info("Assignment violates the SLA");
                return true;
            }
        }
        log.info("Assignment  does NOT  violates the SLA");
        return false;
    }


    private static void resetPartitions(float f) {
        partsReset = new ArrayList<>(ArrivalRates.topicpartitions);
        for (Partition partition : partsReset) {
            if (partition.getLag() > 200 * wsla * f) {
                log.info("Since partition {} has lag {} higher than consumer capacity times wsla {}" +
                        " we are truncating its lag", partition.getId(), partition.getLag(), 200 * wsla * f);
                partition.setLag((long) (200 * wsla * f));
            }
        }
        //if a certain partition has an arrival rate  higher than µ  set its arrival rate  to µ
        //that should not happen in a well partionned topic
        for (Partition partition : partsReset) {
            if (partition.getArrivalRate() > 200 * f) {
                log.info("Since partition {} has arrival rate {} higher than consumer service rate {}" +
                                " we are truncating its arrival rate", partition.getId(),
                        String.format("%.2f", partition.getArrivalRate()),
                        String.format("%.2f", 200f * f));
                partition.setArrivalRate(200f * f);
            }
        }

    }


    private static int binPackAndScale() {
        log.info(" shall we upscale group {}", "testgroup1");
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 1;
        float fraction = 0.9f;
        //start the bin pack FFD with sort
        Collections.sort(partsReset, Collections.reverseOrder());
        while (true) {
            int j;
            consumers.clear();
            for (int t = 0; t < consumerCount; t++) {
                consumers.add(new Consumer((String.valueOf(t)), (long) (200 * wsla * fraction),
                        200 * fraction));
            }

            for (j = 0; j < partsReset.size(); j++) {
                int i;
                Collections.sort(consumers, Collections.reverseOrder());
                for (i = 0; i < consumerCount; i++) {
                    if (consumers.get(i).getRemainingLagCapacity() >= partsReset.get(j).getLag() &&
                            consumers.get(i).getRemainingArrivalCapacity() >= partsReset.get(j).getArrivalRate()) {
                        consumers.get(i).assignPartition(partsReset.get(j));
                        break;
                    }
                }
                if (i == consumerCount) {
                    consumerCount++;
                    break;
                }
            }
            if (j == partsReset.size())
                break;
        }
        assignment = consumers;
        log.info(" The BP up scaler recommended for group {} {}", "testgroup1", consumers.size());
        return consumers.size();
    }


    static int binPackAndScaled() {
        log.info(" shall we down scale group {} ", "testgroup1");
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 1;
        double fractiondynamicAverageMaxConsumptionRate = 200 * 0.4;

        //start the bin pack FFD with sort
        Collections.sort(partsReset, Collections.reverseOrder());
        while (true) {
            int j;
            consumers.clear();
            for (int t = 0; t < consumerCount; t++) {
                consumers.add(new Consumer((String.valueOf(consumerCount)),
                        (long) (fractiondynamicAverageMaxConsumptionRate * wsla),
                        fractiondynamicAverageMaxConsumptionRate));
            }

            for (j = 0; j < partsReset.size(); j++) {
                int i;
                Collections.sort(consumers, Collections.reverseOrder());
                for (i = 0; i < consumerCount; i++) {

                    if (consumers.get(i).getRemainingLagCapacity() >= partsReset.get(j).getLag() &&
                            consumers.get(i).getRemainingArrivalCapacity() >= partsReset.get(j).getArrivalRate()) {
                        consumers.get(i).assignPartition(partsReset.get(j));
                        break;
                    }
                }
                if (i == consumerCount) {
                    consumerCount++;
                    break;
                }
            }
            if (j == partsReset.size())
                break;
        }
        assignment = consumers;

        log.info(" The BP down scaler recommended  for group {} {}", "testgroup1", consumers.size());
        return consumers.size();
    }

}
