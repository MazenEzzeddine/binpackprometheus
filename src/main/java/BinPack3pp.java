
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BinPack3pp {

    //TODO give fup and fdown as paramters to the functions.
    private static final Logger log = LogManager.getLogger(BinPack3pp.class);
    private  int size =1;
    public   Instant LastUpScaleDecision = Instant.now();

    private final double wsla = 1;//0.5;
   static boolean scaled;

    static List<Consumer> assignment =  new ArrayList<Consumer>();



    public  void scaleAsPerBinPack() {
        scaled = false;
        log.info("Currently we have this number of consumers group {} {}","testgroup1", size );

        log.info("We have this processing rate {}", ArrivalRates.processingRate);
        int neededsize = binPackAndScale();
        log.info("We currently need the following consumers for group1 (as per the bin pack) {}", neededsize);
        int replicasForscale = neededsize - size;
        if (replicasForscale > 0) {
            scaled = true;
            //TODO IF and Else IF can be in the same logic
            log.info("We have to upscale  group1 by {}", replicasForscale);
           // neededsize=5;
            size = neededsize;

            LastUpScaleDecision= Instant.now();

            try (final KubernetesClient k8s = new KubernetesClientBuilder().build() ) {
                k8s.apps().deployments().inNamespace("default").withName("latency").scale(neededsize);
                log.info("I have Upscaled group {} you should have {}", "testgroup11", neededsize);
            }

        } else {
            int neededsized = binPackAndScaled();
            int replicasForscaled = size - neededsized;
            if (replicasForscaled > 0) {
               // scaled = true;
                log.info("We have to downscale  group by {} {}", "testgroup1", replicasForscaled);
               // neededsized=5;
                size = neededsized;
                LastUpScaleDecision = Instant.now();

                try (final KubernetesClient k8s = new KubernetesClientBuilder().build()) {
                    k8s.apps().deployments().inNamespace("default").withName("latency").scale(neededsized);
                    log.info("I have downscaled group {} you should have {}", "testgroup1", neededsized);
                }
            }
        }
        log.info("===================================");
    }






    private  int binPackAndScale() {
        log.info(" shall we upscale group {}", "testgroup1");
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 1;
        List<Partition> parts = new ArrayList<>(ArrivalRates.topicpartitions);

        float fraction = 0.9f;//1.0f;//1;//0.9f;//1.0f;//0.9f; //1f;


        //if a certain partition has an arrival rate  higher than R  set its arrival rate  to R
        //that should not happen in a well partionned topic
        for (Partition partition : parts) {
            if (partition.getArrivalRate() >ArrivalRates.processingRate *fraction/*dynamicAverageMaxConsumptionRate*wsla*/) {
                log.info("Since partition {} has arrival rate {} higher than consumer service rate {}" +
                                " we are truncating its arrival rate", partition.getId(),
                        String.format("%.2f", partition.getArrivalRate()),
                        String.format("%.2f",ArrivalRates.processingRate *fraction /*dynamicAverageMaxConsumptionRate*wsla*/));
                partition.setArrivalRate(ArrivalRates.processingRate*fraction /*dynamicAverageMaxConsumptionRate*wsla*/);
            }
        }
        //start the bin pack FFD with sort
        Collections.sort(parts, Collections.reverseOrder());

        while (true) {
            int j;
            consumers.clear();
            for (int t = 0; t < consumerCount; t++) {
                consumers.add(new Consumer((String.valueOf(t)),  (long)(ArrivalRates.processingRate*wsla*fraction),
                        ArrivalRates.processingRate*fraction/*dynamicAverageMaxConsumptionRate*wsla*/));
            }

            for (j = 0; j < parts.size(); j++) {
                int i;
                Collections.sort(consumers, Collections.reverseOrder());
                for (i = 0; i < consumerCount; i++) {

                    if (
                            consumers.get(i).getRemainingArrivalCapacity() >= parts.get(j).getArrivalRate()) {
                        consumers.get(i).assignPartition(parts.get(j));
                        break;
                    }
                }
                if (i == consumerCount) {
                    consumerCount++;
                    break;
                }
            }
            if (j == parts.size())
                break;
        }
        log.info(" The BP up scaler recommended for group {} {}", "testgroup1", consumers.size());
        assignment = consumers;
        return consumers.size();
    }

    private  int binPackAndScaled() {
        log.info(" shall we down scale group {} ", "testgroup1");
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 1;
        List<Partition> parts = new ArrayList<>(ArrivalRates.topicpartitions);
        double fractiondynamicAverageMaxConsumptionRate = ArrivalRates.processingRate*0.4;//*1.0;/**0.5*//**0.7*/ /*dynamicAverageMaxConsumptionRate * 0.7*wsla*/;



        //if a certain partition has an arrival rate  higher than R  set its arrival rate  to R
        //that should not happen in a well partionned topic
        for (Partition partition : parts) {
            if (partition.getArrivalRate() > fractiondynamicAverageMaxConsumptionRate) {
                log.info("Since partition {} has arrival rate {} higher than consumer service rate {}" +
                                " we are truncating its arrival rate", partition.getId(),
                        String.format("%.2f", partition.getArrivalRate()),
                        String.format("%.2f", fractiondynamicAverageMaxConsumptionRate));
                partition.setArrivalRate(fractiondynamicAverageMaxConsumptionRate);
            }
        }
        //start the bin pack FFD with sort
        Collections.sort(parts, Collections.reverseOrder());
        while (true) {
            int j;
            consumers.clear();
            for (int t = 0; t < consumerCount; t++) {
                consumers.add(new Consumer((String.valueOf(t)),
                        (long)(fractiondynamicAverageMaxConsumptionRate*wsla),
                        fractiondynamicAverageMaxConsumptionRate));
            }

            for (j = 0; j < parts.size(); j++) {
                int i;
                Collections.sort(consumers, Collections.reverseOrder());
                for (i = 0; i < consumerCount; i++) {

                    if (
                            consumers.get(i).getRemainingArrivalCapacity() >= parts.get(j).getArrivalRate()) {
                        consumers.get(i).assignPartition(parts.get(j));
                        break;
                    }
                }
                if (i == consumerCount) {
                    consumerCount++;
                    break;
                }
            }
            if (j == parts.size())
                break;
        }
        log.info(" The BP down scaler recommended  for group {} {}", "testgroup1", consumers.size());
        assignment = consumers;
        return consumers.size();
    }


}
