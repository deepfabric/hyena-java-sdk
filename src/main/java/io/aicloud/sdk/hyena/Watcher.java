package io.aicloud.sdk.hyena;

import io.aicloud.tools.netty.ChannelAware;
import io.aicloud.tools.netty.Connector;
import io.aicloud.tools.netty.ConnectorBuilder;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

/**
 * Description:
 * <pre>
 * Date: 2018-10-29
 * Time: 12:48
 * </pre>
 *
 * @author fagongzi
 */
@Slf4j(topic = "hyena")
public class Watcher implements ChannelAware<Object> {
    private int flag;
    private Connector<Object> connector;
    private Router router;

    Watcher(Router router, String... addresses) {
        this(router, EventNotify.EVENT_FLAG_CONTAINER | EventNotify.EVENT_FLAG_RESOURCE | EventNotify.EVENT_INIT, addresses);
    }

    private Watcher(Router router, int flag, String... addresses) {
        this.router = router;
        this.flag = flag;
        connector = new ConnectorBuilder<>(addresses)
                .allowReconnect(true, 5)
                .codec(new EventCodec())
                .channelAware(this)
                .build();
        connector.connect();
    }

    @Override
    public void messageReceived(Channel channel, Object message) {
        if (message instanceof EventNotify) {
            switch (((EventNotify) message).getEvent()) {
                case EventNotify.EVENT_INIT:
                    log.debug("init event: {}", message);
                    router.onInitEvent((EventNotify) message);
                    break;
                case EventNotify.EVENT_RESOURCE_CHANGED:
                    log.debug("db changed event: {}", message);
                    router.onDBCreatedOrChanged((EventNotify) message);
                    break;
                case EventNotify.EVENT_RESOURCE_CREATED:
                    log.debug("db created event: {}", message);
                    router.onDBCreatedOrChanged((EventNotify) message);
                    break;
                case EventNotify.EVENT_RESOURCE_LEADER_CHANGED:
                    log.debug("db leader changed event: {}", message);
                    router.onDBLeaderChanged((EventNotify) message);
                    break;
                case EventNotify.EVENT_CONTAINER_CREATED:
                    log.debug("store created event: {}", message);
                    router.onStoreCreatedOrChanged((EventNotify) message);
                    break;
                case EventNotify.EVENT_CONTAINER_CHANGED:
                    log.debug("store changed event: {}", message);
                    router.onStoreCreatedOrChanged((EventNotify) message);
                    break;
            }
        } else if (message instanceof Error) {
            // choose another server, close current connection
            connector.close();

            log.info("received a error notify {}, closed watcher connection to choose another hyena server",
                    ((Error) message).getErr());
        }
    }

    @Override
    public void onChannelException(Channel channel, Throwable cause) {
        log.error("uncaught exception", cause);
    }

    @Override
    public void onChannelClosed(Channel channel) {
        log.warn("connection {} closed", channel.remoteAddress());
    }

    @Override
    public void onChannelConnected(Channel channel) {
        connector.writeAndFlush(new EventNotify.InitWatcher(flag));

        log.info("connected to hyena succeed, sent {} flag to watch event",
                channel.remoteAddress(),
                flag);
    }
}
