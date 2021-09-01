package exchange.direct;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 *  多个消费者订阅一个队列，消息在队列之间轮询发送。
 */
public class MultiConsumerOneQueue {
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("172.17.5.3");
        Connection connection = connectionFactory.newConnection();

        for (int i = 0; i < 3; i++) {
            Thread thread = new Thread(new ConsumerWorker(connection, "focusAll"));
            thread.start();
        }
    }

    public static class ConsumerWorker implements Runnable {
        private Connection connection;
        private String queueName;

        public ConsumerWorker(Connection connection, String queueName) {
            this.connection = connection;
            this.queueName = queueName;
        }

        @Override
        public void run() {
            try {
                // create channel
                Channel channel = connection.createChannel();
                //  declare  exchange
                channel.exchangeDeclare(DirectProducer.EXCHANGE_NAME, "direct");

                // declare queue
                channel.queueDeclare(queueName, false, false, false, null);

                // binding queue to exchange by routing-key
                String[] routingKeys = {"info", "error", "warning"};
                for (String routingKey : routingKeys) {
                    channel.queueBind(queueName, DirectProducer.EXCHANGE_NAME, routingKey);
                }
                String consumerName = Thread.currentThread().getName();
                System.out.println("[" + consumerName + "] waiting for message.");

                //create consumer
                DefaultConsumer consumer = new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                        String message = new String(body, "UTF-8");
                        System.out.println(consumerName + " received " + envelope.getRoutingKey() + ": " + message);
                    }
                };

                channel.basicConsume(queueName, true, consumer);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
