package testRabbit;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class TestProducer {
    public static final String EXCHANGE_NAME = "direct_log";

    public static void main(String[] args) throws IOException, TimeoutException {
        //  Create connection to rabbitmq
        ConnectionFactory connectionFactory = new ConnectionFactory();
//        connectionFactory.setVirtualHost("test-rabbit");
        connectionFactory.setHost("172.17.5.3");
        Connection connection = connectionFactory.newConnection();

        // create channel
        Channel channel = connection.createChannel();

        // declare exchanger
        channel.exchangeDeclare(EXCHANGE_NAME, "direct");

        // routing key
        String[] routingKeys = {"error", "info", "warning"};
        for (int i = 0; i < 3; i++) {
            String routingKey = routingKeys[i % 3];
            String msg = "Hello Rabbitmq" + (i + 1);

            // publish message
            channel.basicPublish(EXCHANGE_NAME, routingKey, null, msg.getBytes(StandardCharsets.UTF_8));
            System.out.println("Sent " + routingKey + ": " + msg);
        }

        channel.close();
        connection.close();
    }
}
