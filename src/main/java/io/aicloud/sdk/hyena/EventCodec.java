package io.aicloud.sdk.hyena;

import com.alibaba.fastjson.JSONObject;
import io.aicloud.tools.netty.Codec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.Charset;

/**
 * Description:
 * <pre>
 * Date: 2018-10-29
 * Time: 13:09
 * </pre>
 *
 * @author fagongzi
 */
@Slf4j(topic = "hyena")
public class EventCodec implements Codec<Object> {
    private static Charset UTF8 = Charset.forName("UTF-8");
    private static final byte typeErrorRsp = 10;
    private static final byte typeInitWatcher = 11;
    private static final byte typeEventNotify = 12;

    @Override
    public Object decode(byte[] value) {
        Object retValue = null;
        switch (value[0]) {
            case typeInitWatcher:
                retValue = JSONObject.parseObject(value, 1, value.length - 1, UTF8, EventNotify.InitWatcher.class);
                break;
            case typeEventNotify:
                retValue = JSONObject.parseObject(value, 1, value.length - 1, UTF8, EventNotify.class);
                break;
            case typeErrorRsp:
                retValue = JSONObject.parseObject(value, 1, value.length - 1, UTF8, Error.class);
            default:
                log.warn("not support event type {}", value[0]);
                break;
        }
        return retValue;
    }

    @Override
    public ByteBuf encode(ByteBufAllocator allocator, Object value) {
        ByteBuf buf = allocator.buffer();
        if (value instanceof EventNotify.InitWatcher) {
            buf.writeByte(typeInitWatcher);
            buf.writeBytes(JSONObject.toJSONString(value).getBytes(UTF8));
            return buf;
        }

        return null;
    }
}
