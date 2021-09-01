package testRabbit;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 消息消费者
 */
public class TestConsumer {
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("172.17.5.3");
        Connection connection = connectionFactory.newConnection();

        Channel channel = connection.createChannel();
        channel.exchangeDeclare(TestProducer.EXCHANGE_NAME, "direct");

        // declare queue
        String queueName = "focus.error";
        channel.queueDeclare(queueName, false, false, false, null);

        // binding queue to exchange by routing-key
        String routingKey = "error";
        channel.queueBind(queueName, TestProducer.EXCHANGE_NAME, routingKey);
        System.out.println("Waiting for message......");

        // declare consumer
        DefaultConsumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("Received [" + envelope.getRoutingKey() + "] " + message);
            }
        };

        // 消费者正式开始在指定队列上消费消息
        channel.basicConsume(queueName, consumer);
    }
}
