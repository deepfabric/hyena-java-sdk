package io.aicloud.sdk.hyena;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import io.aicloud.sdk.hyena.pb.InsertRequest;
import io.aicloud.sdk.hyena.pb.SearchRequest;
import io.aicloud.sdk.hyena.pb.SearchResponse;
import io.aicloud.sdk.hyena.pb.UpdateRequest;
import io.aicloud.tools.netty.ChannelAware;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;

import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Description:
 * <pre>
 * Date: 2018-10-31
 * Time: 11:10
 * </pre>
 *
 * @author fagongzi
 */
@Slf4j(topic = "hyena")
class MQBasedClient implements Client, ChannelAware<MessageLite> {
    private static Charset UTF8 = Charset.forName("UTF-8");
    private Options options;
    private Watcher watcher;
    private Router router;
    private Map<ByteString, Context> contexts = new ConcurrentHashMap<>(4096);

    private Producer<byte[], byte[]> producer;
    private AtomicLong offset = new AtomicLong(0);

    MQBasedClient(Options options, String... hyenaAddresses) throws InterruptedException {
        log.info("start hyena sdk client with kafka servers: {}, topic: ", options.getBrokers(), options.getTopic());
        log.info("start hyena sdk client with hyena servers: {}", Arrays.toString(hyenaAddresses));
        log.info("start hyena sdk client with options: {}", options);

        this.options = options;
        this.router = new Router(options.getExecutors(), options.getIoExecutors(), this);
        this.watcher = new Watcher(router, hyenaAddresses);

        initMQProducer();
        this.router.waitForComplete();
    }

    private void initMQProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, options.getBrokers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

        producer = new KafkaProducer<>(properties);
    }

    @Override
    public void insert(InsertRequest request) throws Exception {
        doPublishToMQ(request);
    }

    @Override
    public void update(UpdateRequest request) throws Exception {
        doPublishToMQ(request);
    }

    @Override
    public Future<SearchResponse> search(SearchRequest request) throws Exception {
        Context ctx = new Context();
        ctx.reset(request.getXqCount() / options.getDim(), options);

        doSearch(ctx, request);

        return ctx;
    }

    private void doPublishToMQ(MessageLite request) throws ExecutionException, InterruptedException {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(options.getTopic(), null, RPCCodec.DEFAULT.encodeWithLength(request));
        Future<RecordMetadata> value = producer.send(record);
        RecordMetadata metadata = value.get();

        for (; ; ) {
            long oldValue = offset.get();
            if (oldValue >= metadata.offset()) {
                return;
            }

            if (offset.compareAndSet(oldValue, metadata.offset())) {
                return;
            }
        }
    }

    private void doSearch(Context ctx, SearchRequest request) {
        long after = offset.get();
        router.doForEachDB(db -> {
            ctx.to++;
            SearchRequest req = SearchRequest.newBuilder()
                    .setId(ByteString.copyFrom(UUID.randomUUID().toString(), UTF8))
                    .setOffset(after)
                    .setDb(db.getId())
                    .addAllXq(request.getXqList())
                    .build();
            addAsyncContext(req, ctx);
            router.sent(db, req);
        });
    }

    private void addAsyncContext(SearchRequest req, Context ctx) {
        contexts.put(req.getId(), ctx);
    }

    @Override
    public void messageReceived(Channel channel, MessageLite message) {
        if (null == message) {
            return;
        }

        if (message instanceof SearchResponse) {
            Context ctx = contexts.remove(((SearchResponse) message).getId());
            if (null != ctx) {
                ctx.done((SearchResponse) message);
            }
        }
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

    private static class Context implements Future<SearchResponse> {
        private Options options;
        private long to;
        private AtomicLong received;
        private List<Float> distances;
        private List<Long> ids;
        private CountDownLatch latch;
        private long db;

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            throw new UnsupportedOperationException("not support");
        }

        @Override
        public boolean isCancelled() {
            throw new UnsupportedOperationException("not support");
        }

        @Override
        public boolean isDone() {
            throw new UnsupportedOperationException("not support");
        }

        @Override
        public SearchResponse get() throws InterruptedException, ExecutionException {
            return doGet(options.getTimeout(), options.getTimeoutUnit());
        }

        @Override
        public SearchResponse get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return doGet(timeout, unit);
        }

        private SearchResponse doGet(long timeout, TimeUnit unit) throws InterruptedException {
            wait(timeout, unit);
            return SearchResponse.newBuilder()
                    .setDb(db)
                    .addAllDistances(distances)
                    .addAllXids(ids)
                    .build();
        }

        private void reset(int size, Options options) {
            this.options = options;
            latch = new CountDownLatch(1);

            to = 0;
            received = new AtomicLong(0);
            distances = new ArrayList<>(size);
            ids = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                distances.add(0.0f);
                ids.add(-1L);
            }
        }

        private void done(SearchResponse response) {
            long newValue = received.incrementAndGet();
            for (int i = 0; i < response.getXidsCount(); i++) {
                long value = response.getXids(i);
                if (value == -1) {
                    continue;
                }

                synchronized (this) {
                    if (betterThan(response.getDistances(i), distances.get(i))) {
                        distances.set(i, response.getDistances(i));
                        ids.set(i, value);
                        db = response.getDb();
                    }
                }
            }

            if (to == newValue) {
                latch.countDown();
            }
        }

        private void wait(long timeout, TimeUnit unit) throws InterruptedException {
            latch.await(timeout, unit);
        }

        private static boolean betterThan(float source, float target) {
            return source > target;
        }
    }
}
