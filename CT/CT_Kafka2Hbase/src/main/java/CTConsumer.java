

public class CTConsumer {
    public static void main(String[] args) {
        System.out.println(PropertiesUtil.getProperty("kafka.topics"));
        HBaseConsumer hBaseConsumer = new HBaseConsumer();
        hBaseConsumer.getDataFromKafka();
    }
}
