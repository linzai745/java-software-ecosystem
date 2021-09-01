package exchange.direct;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 *  一个连接多个信道
 */
public class MultiChannelConsumer {
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("172.17.5.3");
        Connection connection = connectionFactory.newConnection();

        for (int i = 0; i < 2; i++) {
            Thread thread = new Thread(new ConsumerWorker(connection));
            thread.start();
        }
    }
    public static class ConsumerWorker implements Runnable {
        final Connection connection;

        public ConsumerWorker(Connection connection) {
            this.connection = connection;
        }

        @Override
        public void run() {
            try {
                // create channel for every thread
                final Channel channel = connection.createChannel();
                // declare exchange
                channel.exchangeDeclare(DirectProducer.EXCHANGE_NAME, "direct");
                //  declare  a random queue
                String queueName = channel.queueDeclare().getQueue();

                // binding queue to exchange
                String[] routingKeys = {"info", "error", "warning"};
                for (String routingKey :  routingKeys) {
                    channel.queueBind(queueName, DirectProducer.EXCHANGE_NAME, routingKey);
                }

                String consumerName = Thread.currentThread().getName() + "-all";
                System.out.println("["+ consumerName +"]" +" waiting for message");

                // create consumer
                DefaultConsumer consumer = new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                        String message = new String(body, "UTF-8");
                        System.out.println(consumerName + " Received " + envelope.getRoutingKey() + ": " + message);
                    }
                };

                // consume message
                channel.basicConsume(queueName, true, consumer);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
