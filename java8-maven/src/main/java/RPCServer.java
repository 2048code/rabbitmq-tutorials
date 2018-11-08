import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RPCServer {

    private static final String RPC_QUEUE_NAME = "rpc_queue2";

    private static int fib(int n) {
        if (n == 0) return 0;
        if (n == 1) return 1;
        return fib(n - 1) + fib(n - 2);
    }

    public static void main(String[] argv) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("52.221.180.102");
        factory.setPort(5672);
        factory.setUsername("admin");
        factory.setPassword("rabbit@admin123");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            // 声明一个队列
            // 队列名称，持久化，是否独占队列，当所有消费者客户端断开时是否自动删除队列
            channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);
            //每次从队列获取的数量
            channel.basicQos(1);

            System.out.println(" [x] Awaiting RPC requests");
            // DefaultConsumer类实现了Consumer接口，通过传入一个频道，
            // 如果频道有消息，就会执行回调的handleDelivery
            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                            .Builder()
                            .correlationId(properties.getCorrelationId())
                            .build();

                    String response = "aaaa";

                    try {
                        String message = new String(body, "UTF-8");
                        int n = Integer.parseInt(message);

                        System.out.println(" [.] fib(" + message + ")");
                        response += fib(n);
                    } catch (RuntimeException e) {
                        System.out.println(" [.RuntimeException] " + e.toString());
                    } finally {
                        try {
                            Thread.sleep(5000);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        System.out.println("basicPublish");
                        // 发送消息到队列中
                        // 交换机名称 队列映射的路由key，消息其他属性，数据
                        channel.basicPublish("", properties.getReplyTo(), replyProps, response.getBytes("UTF-8"));
                        channel.basicAck(envelope.getDeliveryTag(), false);
                        // RabbitMq consumer worker thread notifies the RPC server owner thread
                        synchronized (this) {
                            this.notify();
                        }
                    }
                }
            };
            // 消息消费完成确认
            // 自动回复队列应答 -- RabbitMQ中的消息确认机制
            channel.basicConsume(RPC_QUEUE_NAME, false, consumer);
            // Wait and be prepared to consume the message from RPC client.
            while (true) {
                synchronized (consumer) {
                    try {
                        consumer.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }
}