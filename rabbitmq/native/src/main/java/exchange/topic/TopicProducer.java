package exchange.topic;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

// topic 交换器
public class TopicProducer {
    public static final String EXCHANGE_NAME = "topic_logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("172.17.5.3");
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

        String[] levels = {"error", "info", "warning"};
        String[] modules = {"user", "order", "email"};
        String[] services = {"A", "B", "C"};

        for (int i = 0; i < levels.length; i++) {
            for (int j = 0; j < modules.length; j++) {
                for (int k = 0; k < services.length; k++) {
                    String message = "Hello Topic_["+i+","+j+","+k+"]";
                    String routingKey = levels[i] + "." + modules[j] + "." + services[k];
                    channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes(StandardCharsets.UTF_8));
                    System.out.println("Sent " + routingKey + " message: " + message );
                }
            }
        }

        channel.close();;
        connection.close();
    }
}
