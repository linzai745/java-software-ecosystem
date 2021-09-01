package exchange.fanout;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * fanout exchange
 */
public class FanoutProducer {
    public static final String EXCHANGE_NAME = "fanout_logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        // create connection
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("172.17.5.3");
        Connection connection = connectionFactory.newConnection();

        // create channel
        Channel channel = connection.createChannel();
        // declare exchange
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
        //  declare queue and binding to exchange
        String queueName = "producer_create";
        channel.queueDeclare(queueName, false, false, false, null);
        channel.queueBind(queueName, EXCHANGE_NAME, "test");
        // 发布消息
        String[] routingKeys = {"info", "error", "warning"};
        for (int i = 0 ; i < routingKeys.length; i++) {
            String routingKey = routingKeys[i % 3];
            String msg = "Hello, RabbitMQ" + (i + 1);
            channel.basicPublish(EXCHANGE_NAME, routingKey, null, msg.getBytes(StandardCharsets.UTF_8));
            System.out.println("[x] Sent '"  + routingKey + "':'" + msg + "'");
        }
        channel.close();
        connection.close();
    }
}
