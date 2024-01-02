import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BinPackRestructureWithLagLag {

    private static final Logger log = LogManager.getLogger(BinPackRestructureWithLagLag.class);
    public static int size = 1;
    static double wsla = 0.5;
    static double rebTime = 2.0;
    static List<Consumer> assignment = new ArrayList<Consumer>();
    static List<Consumer> currentAssignment = assignment;
    static List<Partition> partsReset;
    private static KafkaConsumer<byte[], byte[]> metadataConsumer;


    static double mu = 200.0;

    static {
        currentAssignment.add(new Consumer("0", (long) (mu * wsla * .9),
                mu * .9));
        for (Partition p : ArrivalRates.topicpartitions) {
            currentAssignment.get(0).assignPartition(p);
        }
    }

    public Instant LastUpScaleDecision = Instant.now();

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
            log.info("consumer {}", cons.getId());
            for (Partition p : cons.getAssignedPartitions()) {
                log.info("partition {}", p.getId());
                sumPartitionsArrival += ArrivalRates.topicpartitions.get(p.getId()).getArrivalRate();
                sumPartitionsLag += ArrivalRates.topicpartitions.get(p.getId()).getLag();
            }
            double arrivalwhileprocessing = sumPartitionsLag / (mu * 0.9) * sumPartitionsArrival;

            if ((sumPartitionsLag + arrivalwhileprocessing) > (wsla * mu * .9f)
                /*|| sumPartitionsArrival >=mu * 0.9f*/) {
                return true;
            }
        }
        return false;
    }

    private static void resetPartitions(float f) {
        partsReset = new ArrayList<>(ArrivalRates.topicpartitions);
        for (Partition partition : partsReset) {
            if (partition.getLag() > mu * wsla * f) {
                log.info("Since partition {} has lag {} higher than consumer capacity times wsla {}" + " we are truncating its lag",
                        partition.getId(), partition.getLag(), mu * wsla * f);
                partition.setLag((long) (mu * wsla * f));
            }
        }
        for (Partition partition : partsReset) {
            if (partition.getArrivalRate() > mu * f) {
                log.info("Since partition {} has arrival rate {} higher than consumer service rate {}" +
                                " we are truncating its arrival rate", partition.getId(),
                        String.format("%.2f", partition.getArrivalRate()), String.format("%.2f", mu * f));
                partition.setArrivalRate(mu * f);
            }
        }
    }


    private static int binPackAndScale() {
        log.info(" shall we upscale group {}", "testgroup1");
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 1;
        float fraction = 0.9f;
        Collections.sort(partsReset, Collections.reverseOrder());

        while (true) {
            int j;
            consumers.clear();
            for (int t = 0; t < consumerCount; t++) {
                consumers.add(new Consumer((String.valueOf(t)), (long) (mu * wsla * fraction), mu * fraction));
            }
            for (j = 0; j < partsReset.size(); j++) {
                int i;
                Collections.sort(consumers, Collections.reverseOrder());
                for (i = 0; i < consumerCount; i++) {
                    if (consumers.get(i).getRemainingLagCapacity() >= partsReset.get(j).getLag() &&
                            consumers.get(i).getRemainingArrivalCapacity() >= partsReset.get(j).getArrivalRate() &&
                            isOK(consumers.get(i), partsReset.get(j), fraction)) {
                        consumers.get(i).assignPartition(partsReset.get(j));
                        break;
                    }
                }
                if (i == consumerCount) {
                    consumerCount++;
                    break;
                }
            }
            if (j == partsReset.size()) break;
        }
        assignment = consumers;
        log.info(" The BP up scaler recommended for group {} {}", "testgroup1", consumers.size());
        return consumers.size();
    }



   //can we assign this partition to thsi consumer
    private static boolean isOK(Consumer consumer, Partition partition, double f) {

        log.info("consumer {}", consumer.getId());
        double sumPartitionsArrival = 0;
        double sumPartitionsLag = 0;

        // check
        // what shall we do when a partition lag and the arrival while processing lag is greater
        // than µ*wsla*f

         double arrivalTopartition = partition.getLag()/(mu * f)* partition.getArrivalRate();
        if(arrivalTopartition + partition.getLag() >= (mu * f)*wsla) {

            partsReset.get(partition.getId()).setLag((long)(partition.getLag() - arrivalTopartition));
        }

        for (Partition p : consumer.getAssignedPartitions()) {
            sumPartitionsArrival += ArrivalRates.topicpartitions.get(p.getId()).getArrivalRate();
            sumPartitionsLag += ArrivalRates.topicpartitions.get(p.getId()).getLag();
        }

        log.info("sumPartitionsArrival {}", sumPartitionsArrival);
        log.info("sumPartitionsLag {}", sumPartitionsLag);
        double arrivalwhileprocessing = (sumPartitionsLag + partition.getLag()) / (mu * f) *
                (sumPartitionsArrival + partition.getArrivalRate());

        log.info("arrivalwhileprocessing {}", arrivalwhileprocessing);
        log.info("partition.getLag() {}", partition.getLag());
        double total = partition.getLag() + arrivalwhileprocessing + sumPartitionsLag;

        if (total <= mu * wsla * f) {
            return true;
        }
        //  log.info("false");
        return false;
    }










    static int binPackAndScaled() {
        log.info(" shall we down scale group {} ", "testgroup1");
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 1;
        double fractiondynamicAverageMaxConsumptionRate = mu * 0.4;

        //start the bin pack FFD with sort
        Collections.sort(partsReset, Collections.reverseOrder());
        while (true) {
            int j;
            consumers.clear();
            for (int t = 0; t < consumerCount; t++) {
                consumers.add(new Consumer((String.valueOf(t)),
                        (long) (fractiondynamicAverageMaxConsumptionRate * wsla),
                        fractiondynamicAverageMaxConsumptionRate));
            }

            for (j = 0; j < partsReset.size(); j++) {
                int i;
                Collections.sort(consumers, Collections.reverseOrder());
                for (i = 0; i < consumerCount; i++) {

                    if (consumers.get(i).getRemainingLagCapacity() >= partsReset.get(j).getLag()
                            && consumers.get(i).getRemainingArrivalCapacity() >= partsReset.get(j).getArrivalRate() &&
                            isOK(consumers.get(i), partsReset.get(j), 0.4)) {
                        consumers.get(i).assignPartition(partsReset.get(j));
                        break;
                    }
                }
                if (i == consumerCount) {
                    consumerCount++;
                    break;
                }
            }
            if (j == partsReset.size()) break;
        }
        assignment = consumers;
        log.info(" The BP down scaler recommended  for group {} {}", "testgroup1", consumers.size());
        return consumers.size();
    }

}
