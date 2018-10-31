package io.aicloud.sdk.hyena;

import com.google.protobuf.MessageLite;
import io.aicloud.sdk.hyena.pb.SearchRequest;
import io.aicloud.tools.netty.ChannelAware;
import io.aicloud.tools.netty.Connector;
import io.aicloud.tools.netty.ConnectorBuilder;
import io.aicloud.tools.netty.util.NamedThreadFactory;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.pool2.ObjectPool;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

/**
 * Description:
 * <pre>
 * Date: 2018-10-30
 * Time: 12:59
 * </pre>
 *
 * @author fagongzi
 */
public class Transport implements ChannelAware<SearchRequest> {
    private volatile boolean running = false;
    private ExecutorService executor;
    private ScheduledExecutorService sharedExecutor = Executors.newSingleThreadScheduledExecutor();
    private EventLoopGroup sharedConnectorEventGroup;

    private long mask;
    private Function<Long, String> addressGetter;
    private List<Queue<SentData>> queues;
    private SentData stopFlag = new SentData();

    private ObjectPool<SentData> pool = SimpleBeanPoolFactory.create(SentData::new);

    Transport(int corePoolSize, int ioPoolSize, Function<Long, String> addressGetter) {
        this.addressGetter = addressGetter;
        mask = corePoolSize - 1;
        executor = Executors.newFixedThreadPool(corePoolSize, new NamedThreadFactory("transport"));
        sharedConnectorEventGroup = new NioEventLoopGroup(ioPoolSize);

        queues = new ArrayList<>(corePoolSize);
        for (int i = 0; i < corePoolSize; i++) {
            queues.add(new LinkedBlockingQueue<>());
        }
    }

    void start() {
        running = true;
        queues.forEach(queue -> executor.execute(() -> run(queue)));
    }

    void stop() {
        running = false;
        queues.forEach(queue -> queue.add(stopFlag));
    }

    void sent(long to, SearchRequest message) {
        Queue<SentData> queue = queues.get((int) (mask & to));
        SentData data;
        try {
            data = pool.borrowObject();
        } catch (Exception e) {
            data = new SentData();
        }

        data.setTo(to);
        data.setMessage(message);
        queue.add(data);
    }

    private void run(Queue<SentData> queue) {
        Map<Long, Connector<SearchRequest>> connectors = new HashMap<>();

        while (running) {
            SentData data = queue.poll();
            if (data == stopFlag) {
                break;
            }

            Connector<SearchRequest> conn = connectors.get(data.getTo());
            if (null == conn) {
                String address = addressGetter.apply(data.to);

                if (null == address) {
                    continue;
                }

                connectors.put(data.getTo(), new ConnectorBuilder<SearchRequest>(address)
                        .allowReconnect(true, 5)
                        .channelAware(this)
                        .codec(RPCCodec.DEFAULT)
                        .executor(sharedExecutor)
                        .eventGroup(sharedConnectorEventGroup)
                        .build());
            }
        }
    }

    @Override
    public void messageReceived(Channel channel, SearchRequest message) {

    }

    @Override
    public void onChannelException(Channel channel, Throwable cause) {

    }

    @Override
    public void onChannelClosed(Channel channel) {

    }

    @Override
    public void onChannelConnected(Channel channel) {

    }


    @Getter
    @Setter
    private static class SentData {
        private long to;
        private MessageLite message;
    }
}
