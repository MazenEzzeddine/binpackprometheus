import java.util.ArrayList;
import java.util.List;

public class Consumer implements Comparable<Consumer> {
    private final Long lagCapacity;
    private final double arrivalCapacity;
    private final String id;
    private double remainingArrivalCapacity;
    private List<Partition> assignedPartitions;
    private Long remainingLagCapacity;

    public Consumer(String id, Long lagCapacity,
                    double arrivalCapacity) {
        this.lagCapacity = lagCapacity;
        this.arrivalCapacity = arrivalCapacity;
        this.id = id;
        this.remainingLagCapacity = lagCapacity;
        this.remainingArrivalCapacity = arrivalCapacity;
        assignedPartitions = new ArrayList<>();
    }

    public String getId() {
        return id;
    }
    public Long getRemainingLagCapacity() {
        return remainingLagCapacity;
    }
    public double getRemainingArrivalCapacity() {
        return remainingArrivalCapacity;
    }


    public void assignPartition(Partition partition) {
        assignedPartitions.add(partition);
        remainingLagCapacity -= partition.getLag();
        remainingArrivalCapacity -= partition.getArrivalRate();
    }

    @Override
    public String toString() {
        return "\nConsumer{" + "id=" + id +
                ",  lagCapacity= " + lagCapacity +
                ", remainingArrivalCapacity= " + String.format("%.2f", remainingArrivalCapacity) +
                ", arrivalCapacity= " + String.format("%.2f", arrivalCapacity) +
                ", remainingLagCapacity= " + remainingLagCapacity +
                ", assignedPartitions= \n" + assignedPartitions +
                "}";
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = lagCapacity != null ? lagCapacity.hashCode() : 0;
        temp = Double.doubleToLongBits(remainingArrivalCapacity);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (assignedPartitions != null ? assignedPartitions.hashCode() : 0);
        temp = Double.doubleToLongBits(arrivalCapacity);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (remainingLagCapacity != null ? remainingLagCapacity.hashCode() : 0);
        return result;
    }


    public List<Partition> getAssignedPartitions() {
        return assignedPartitions;
    }

    @Override
    public int compareTo(Consumer o) {
        return Double.compare(this.remainingArrivalCapacity , o.remainingArrivalCapacity);
    }
}
