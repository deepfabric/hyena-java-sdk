package io.aicloud.sdk.hyena;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import io.aicloud.sdk.hyena.pb.*;
import io.aicloud.tools.netty.ChannelAware;
import io.netty.channel.Channel;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Hex;
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
    static Charset UTF8 = Charset.forName("UTF-8");
    private Options options;
    private Watcher watcher;
    private Router router;
    private Timer timeWheel = new HashedWheelTimer(10, TimeUnit.MICROSECONDS);
    private Map<ByteString, Context> contexts = new ConcurrentHashMap<>(4096);
    private Map<ByteString, SearchRequest> requests = new ConcurrentHashMap<>(4096);


    private Producer<byte[], byte[]> producer;
    private AtomicLong offset = new AtomicLong(0);

    MQBasedClient(Options options, String... hyenaAddresses) throws InterruptedException {
        log.info("start hyena sdk client with kafka servers: {}, topic: {}", options.getBrokers(), options.getTopic());
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
    public long insert(InsertRequest request) throws Exception {
        return doPublishToMQ(request);
    }

    @Override
    public long update(UpdateRequest request) throws Exception {
        return doPublishToMQ(request);
    }

    @Override
    public Future search(SearchRequest request) throws Exception {
        Context ctx = new Context(options);
        ctx.reset(request.getXqCount() / options.getDim());

        doSearch(ctx, request);

        return ctx;
    }

    private long doPublishToMQ(MessageLite request) throws ExecutionException, InterruptedException {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(options.getTopic(), null, RPCCodec.DEFAULT.encodeWithLength(request));
        java.util.concurrent.Future<RecordMetadata> value = producer.send(record);
        RecordMetadata metadata = value.get();

        for (; ; ) {
            long oldValue = offset.get();
            if (oldValue >= metadata.offset()) {
                break;
            }

            if (offset.compareAndSet(oldValue, metadata.offset())) {
                break;
            }
        }

        return metadata.offset();
    }

    private void doSearch(Context ctx, SearchRequest request) {
        long after = offset.get();
        log.debug("send search request after {} offset", after);
        ctx.offset = after;
        router.doForEachDB(db -> {
            ctx.to++;
            ctx.sentTo.add(db.getId());
            SearchRequest req = SearchRequest.newBuilder()
                    .setId(ByteString.copyFrom(UUID.randomUUID().toString(), UTF8))
                    .setOffset(after)
                    .setDb(db.getId())
                    .addAllXq(request.getXqList())
                    .setLast(db.getId() == router.getMaxDB())
                    .build();
            addAsyncContext(req, ctx);
            router.sent(db, req);
        });
    }

    void addAsyncContext(SearchRequest req, Context ctx) {
        contexts.put(req.getId(), ctx);
        requests.put(req.getId(), req);
        timeWheel.newTimeout(timeout -> {
            requests.remove(req.getId());
            contexts.remove(req.getId());
        }, options.getTimeout(), options.getTimeoutUnit());
    }

    @Override
    public void messageReceived(Channel channel, MessageLite message) {
        if (null == message) {
            return;
        }

        if (message instanceof SearchResponse) {
            ByteString id = ((SearchResponse) message).getId();
            Context ctx = contexts.remove(id);
            if (null != ctx) {
                if (((SearchResponse) message).getSearchNext()) {
                    log.debug("offset {} search next return by db {}, uuid {}, channel {}",
                            ctx.offset,
                            ((SearchResponse) message).getDb(),
                            Hex.encodeHexString(((SearchResponse) message).getId().toByteArray()),
                            channel);
                    ctx.wait = true;
                    SearchRequest req = requests.get(id);
                    if (req != null) {
                        router.sentWaitReq(((SearchResponse) message).getDb(), req, newReq -> {
                            ctx.to++;
                            ctx.sentTo.add(newReq.getDb());
                            addAsyncContext(newReq, ctx);
                        }, value -> ctx.wait = false);
                    } else {
                        log.debug("offset {} missing request in search next", ctx.offset);
                    }
                }
                ctx.done((SearchResponse) message);
            }
        } else if (message instanceof ErrResponse) {
            log.debug("receive err response {}", message);
            SearchRequest req = requests.get(((ErrResponse) message).getId());

            if (req == null) {
                return;
            }

            // retry
            router.sent(router.getDB(req.getDb()), req);
        }
    }

    @Override
    public void onChannelException(Channel channel, Throwable cause) {
        log.error("channel {} closed", channel, cause);
    }

    @Override
    public void onChannelClosed(Channel channel) {
        log.info("channel {} closed", channel);
    }

    @Override
    public void onChannelConnected(Channel channel) {

    }

    private static class Context implements Future {
        private Options options;
        private volatile boolean wait;
        private volatile long to;
        private AtomicLong received;
        private List<Float> distances;
        private List<Long> ids;
        private CountDownLatch latch;
        private long db;

        private long offset;
        private List<Long> sentTo = Collections.synchronizedList(new ArrayList<>());
        private List<Long> recv = Collections.synchronizedList(new ArrayList<>());

        public Context(Options options) {
            this.options = options;
        }

        @Override
        public SearchResponse get() throws InterruptedException, TimeoutException {
            if (!wait(options.getTimeout(), options.getTimeoutUnit())) {
                throw new TimeoutException("timeout wait response: sent to {" + sentTo + "}, received {" + recv + "}");
            }

            return SearchResponse.newBuilder()
                    .setDb(db)
                    .addAllDistances(distances)
                    .addAllXids(ids)
                    .build();
        }

        private void reset(int size) {
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
            if (null != response) {
                recv.add(response.getDb());
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
            }

            if (to == newValue && !wait) {
                latch.countDown();
            }
        }

        private boolean wait(long timeout, TimeUnit unit) throws InterruptedException {
            return latch.await(timeout, unit);
        }

        private static boolean betterThan(float source, float target) {
            return source > target;
        }
    }
}
