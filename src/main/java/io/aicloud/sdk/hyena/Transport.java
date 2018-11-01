package io.aicloud.sdk.hyena;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import io.aicloud.sdk.hyena.pb.SearchRequest;
import io.aicloud.sdk.hyena.pb.Store;
import io.aicloud.tools.netty.ChannelAware;
import io.aicloud.tools.netty.Connector;
import io.aicloud.tools.netty.ConnectorBuilder;
import io.aicloud.tools.netty.util.NamedThreadFactory;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.ObjectPool;

import java.util.*;
import java.util.concurrent.*;

/**
 * Description:
 * <pre>
 * Date: 2018-10-30
 * Time: 12:59
 * </pre>
 *
 * @author fagongzi
 */
@Slf4j(topic = "hyena")
class Transport {
    private volatile boolean running = false;
    private ExecutorService executor;
    private ScheduledExecutorService sharedExecutor = Executors.newSingleThreadScheduledExecutor();
    private EventLoopGroup sharedConnectorEventGroup;

    private long mask;
    private List<BlockingQueue<SentData>> queues;
    private SentData stopFlag = new SentData();

    private ChannelAware<MessageLite> channelAware;
    private Map<Long, Connector<MessageLite>> connectors = new ConcurrentHashMap<>();
    private ObjectPool<SentData> pool = SimpleBeanPoolFactory.create(SentData::new);

    Transport(int executors, int ioExecutors, ChannelAware<MessageLite> channelAware) {
        log.info("transport start with {} executors and {} io executors",
                executors,
                ioExecutors);

        this.channelAware = channelAware;
        mask = executors - 1;
        executor = Executors.newFixedThreadPool(executors, new NamedThreadFactory("transport"));
        sharedConnectorEventGroup = new NioEventLoopGroup(ioExecutors);

        queues = new ArrayList<>(executors);
        for (int i = 0; i < executors; i++) {
            queues.add(new LinkedBlockingQueue<>());
        }
    }

    void start() {
        running = true;
        final int[] index = {0};
        queues.forEach(queue -> executor.execute(() -> run(queue, index[0]++)));

        log.info("transport started");
    }

    void stop() {
        running = false;
        queues.forEach(queue -> queue.add(stopFlag));

        log.info("transport stopped");
    }

    void addConnector(Store store) {
        if (connectors.containsKey(store.getId())) {
            return;
        }

        Connector<MessageLite> conn = new ConnectorBuilder<MessageLite>(store.getClientAddress())
                .allowReconnect(true, 5)
                .channelAware(channelAware)
                .codec(RPCCodec.DEFAULT)
                .executor(sharedExecutor)
                .eventGroup(sharedConnectorEventGroup)
                .build();
        conn.connect();
        connectors.put(store.getId(), conn);

        log.info("connector for store-{}({}) added",
                store.getId(),
                store.getClientAddress());
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
        data.setId(message.getId());
        queue.add(data);

        log.info("request-{} added to sent queue",
                Arrays.toString(message.getId().toByteArray()));
    }

    private void run(BlockingQueue<SentData> queue, int index) {
        while (running) {
            SentData data = null;
            try {
                data = queue.take();
                if (data == stopFlag) {
                    break;
                }

                Connector<MessageLite> conn = connectors.get(data.getTo());
                if (null != conn) {
                    conn.writeAndFlush(data.getMessage());

                    log.info("request-{} sent succeed",
                            Arrays.toString(data.getId().toByteArray()));
                } else {
                    log.warn("request-{} sent failed with no connector",
                            Arrays.toString(data.getId().toByteArray()));
                }
            } catch (Throwable e) {
                log.error("sent executor-{} failed", e);
            } finally {
                try {
                    pool.returnObject(data);
                } catch (Exception e) {
                    log.error("return sent-data object to poll failed", e);
                }
            }
        }

        log.debug("sent executor-{} exit",
                index);
    }


    @Getter
    @Setter
    private static class SentData {
        private ByteString id;
        private long to;
        private MessageLite message;
    }
}
