package exchange.fanout;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;


// fanout exchange consumer
public class ConsumerA {
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("172.17.5.3");

        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(FanoutProducer.EXCHANGE_NAME, "fanout");
        // declare random queue
        String queueName = channel.queueDeclare().getQueue();
        String[] routingKeys = {"info", "error", "warning"};
        for (String routingKey : routingKeys) {
            channel.queueBind(queueName, FanoutProducer.EXCHANGE_NAME, routingKey);
        }
        System.out.println("[*] Waiting for message.");
        DefaultConsumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("Received " + envelope.getRoutingKey() + ": " + message);
            }
        };
        channel.basicConsume(queueName, consumer);
    }
}
