package exchange.direct;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 一个队列绑定多个路由键
 */
public class MultiBindConsumer {
    public static void main(String[] args) throws IOException, TimeoutException {
        //create connection
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("172.17.5.3");
        Connection connection = connectionFactory.newConnection();

        // create channel
        Channel channel = connection.createChannel();
        // declare exchange
        channel.exchangeDeclare(DirectProducer.EXCHANGE_NAME, "direct");
        // declare  queue (random queue)
        String queueName = channel.queueDeclare().getQueue();
        // binding queue to exchange
        /*队列绑定到交换器上时，是允许绑定多个路由键的，也就是多重绑定*/
        String[] routingKeys = {"info", "error", "warning"};
        for (String routingKey : routingKeys) {
            channel.queueBind(queueName, DirectProducer.EXCHANGE_NAME,  routingKey);
        }
        System.out.println("[*] waiting for messages.");
        // create consumer
        DefaultConsumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("Received " + envelope.getRoutingKey() + ": " + message);
            }
        };

        channel.basicConsume(queueName, true, consumer);
    }
}
