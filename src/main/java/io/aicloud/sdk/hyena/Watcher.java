package io.aicloud.sdk.hyena;

import io.aicloud.tools.netty.ChannelAware;
import io.aicloud.tools.netty.Connector;
import io.aicloud.tools.netty.ConnectorBuilder;
import io.netty.channel.Channel;

/**
 * Description:
 * <pre>
 * Date: 2018-10-29
 * Time: 12:48
 * </pre>
 *
 * @author fagongzi
 */
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
                    router.onInitEvent((EventNotify) message);
                    break;
                case EventNotify.EVENT_RESOURCE_CHANGED:
                    router.onDBCreatedoOrChanged((EventNotify) message);
                    break;
                case EventNotify.EVENT_RESOURCE_CREATED:
                    router.onDBCreatedoOrChanged((EventNotify) message);
                    break;
                case EventNotify.EVENT_RESOURCE_LEADER_CHANGED:
                    router.onDBLeaderChanged((EventNotify) message);
                    break;
                case EventNotify.EVENT_CONTAINER_CREATED:
                    router.onStoreCreatedOrChanged((EventNotify) message);
                    break;
                case EventNotify.EVENT_CONTAINER_CHANGED:
                    router.onStoreCreatedOrChanged((EventNotify) message);
                    break;
            }
        } else if (message instanceof Error) {
            // choose another server, close current connection
            connector.close();
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
        connector.writeAndFlush(new EventNotify.InitWatcher(flag));
    }
}
