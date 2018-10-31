package io.aicloud.sdk.hyena;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import lombok.Getter;
import lombok.Setter;

import java.util.function.Consumer;

/**
 * Description:
 * <pre>
 * Date: 2018-10-26
 * Time: 14:38
 * </pre>
 *
 * @author fagongzi
 */
@Getter
@Setter
public class EventNotify {
    public static final int EVENT_INIT = 1 << 1;
    public static final int EVENT_RESOURCE_CREATED = 1 << 2;
    public static final int EVENT_RESOURCE_LEADER_CHANGED = 1 << 3;
    public static final int EVENT_RESOURCE_CHANGED = 1 << 4;
    public static final int EVENT_CONTAINER_CREATED = 1 << 5;
    public static final int EVENT_CONTAINER_CHANGED = 1 << 6;

    public static final int EVENT_FLAG_RESOURCE = EVENT_RESOURCE_CREATED | EVENT_RESOURCE_LEADER_CHANGED | EVENT_RESOURCE_CHANGED;
    public static final int EVENT_FLAG_CONTAINER = EVENT_CONTAINER_CREATED | EVENT_CONTAINER_CHANGED;
    public static final int EVENT_FLAG_ALL = 0xffffffff;

    private int event;
    private byte[] value;

    void readInitEventValues(Consumer<ResourceValue> resource, Consumer<byte[]> container) {
        if (null == value || value.length == 0) {
            return;
        }

        ByteBuf value = PooledByteBufAllocator.DEFAULT.buffer();

        try {
            value.writeBytes(this.value);

            int rn = value.readInt();
            int cn = value.readInt();

            for (int i = 0; i < rn; i++) {
                int size = value.readInt();
                byte[] data = new byte[size - 8];
                value.readBytes(data);
                long resourceLeader = value.readLong();
                resource.accept(new ResourceValue(data, resourceLeader));
            }

            for (int i = 0; i < cn; i++) {
                int size = value.readInt();
                byte[] data = new byte[size];
                value.readBytes(data);
                container.accept(data);
            }
        } finally {
            ReferenceCountUtil.release(value);
        }
    }

    LeaderChangeValue readLeaderChangeValue() {
        if (null == value || value.length == 0) {
            return null;
        }

        ByteBuf value = PooledByteBufAllocator.DEFAULT.buffer();
        try {
            value.writeBytes(this.value);
            return new LeaderChangeValue(value.readLong(), value.readLong());
        } finally {
            ReferenceCountUtil.release(value);
        }
    }

    @Getter
    @Setter
    public static class InitWatcher {
        private int flag;

        public InitWatcher() {
        }

        InitWatcher(int flag) {
            this.flag = flag;
        }
    }

    @Getter
    @Setter
    static class ResourceValue {
        private byte[] data;
        private long leader;

        ResourceValue(byte[] data, long leader) {
            this.data = data;
            this.leader = leader;
        }
    }

    @Getter
    @Setter
    static class LeaderChangeValue {
        private long resourceId;
        private long newLeaderId;

        LeaderChangeValue(long resourceId, long newLeaderId) {
            this.resourceId = resourceId;
            this.newLeaderId = newLeaderId;
        }
    }
}