import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

public class RPCClient implements AutoCloseable {

    private Connection connection;
    private Channel channel;
    private String requestQueueName = "rpc_queue2";
    private String replyQueueName;

    public RPCClient() throws IOException, TimeoutException {
        System.out.println("RPCClient构造器");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("52.221.180.102");
        factory.setPort(5672);
        factory.setUsername("admin");
        factory.setPassword("rabbit@admin123");


        connection = factory.newConnection();
        System.out.println("RPCClient---connection" + connection);
        channel = connection.createChannel();
        System.out.println("RPCClient---channel"+channel);
        replyQueueName = channel.queueDeclare().getQueue();
        System.out.println("RPCClient---replyQueueName" + replyQueueName);
    }

    public String call(String message) throws IOException, InterruptedException {
        final String corrId = UUID.randomUUID().toString();
        System.out.println("call---corrId" + corrId);
        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId)
                .replyTo(replyQueueName)
                .build();

        channel.basicPublish("", requestQueueName, props, message.getBytes("UTF-8"));

        final BlockingQueue<String> response = new ArrayBlockingQueue<String>(1);
        System.out.println("call---response" + response);
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                if (properties.getCorrelationId().equals(corrId)) {
                    System.out.println("handleDelivery---corrId" + corrId);
                    System.out.println("handleDelivery---consumerTag" + consumerTag);
                    System.out.println("handleDelivery---envelope" + envelope);
                    System.out.println("handleDelivery---properties" + properties);
                    response.offer(new String(body, "UTF-8"));
                }
            }
        };
        channel.basicConsume(replyQueueName, true, consumer);
        return response.take();
    }

    public void close() throws IOException {
        connection.close();
    }

    public static void main(String[] argv) {
        System.out.println("main---corrId" );
        try (RPCClient fibonacciRpc = new RPCClient()) {
            System.out.println("main---fibonacciRpc" + fibonacciRpc);
            System.out.println(" [x] Requesting fib(30)");
            for (int i = 0; i < 10; i++) {
                // 开始時間
                long startTime = System.currentTimeMillis();
                System.out.println("main--- fibonacciRpc.call 前");

                final String response = fibonacciRpc.call(String.valueOf(i));
                System.out.println("main--- fibonacciRpc.call 后" + (System.currentTimeMillis() - startTime));
                System.out.println(" [.] Got '" + response + "'");

            }
        } catch (IOException | TimeoutException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}

