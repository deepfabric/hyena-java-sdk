package io.aicloud.sdk.hyena;

import com.google.protobuf.MessageLite;
import io.aicloud.sdk.hyena.pb.ErrResponse;
import io.aicloud.sdk.hyena.pb.MsgType;
import io.aicloud.sdk.hyena.pb.SearchRequest;
import io.aicloud.sdk.hyena.pb.SearchResponse;
import io.aicloud.tools.netty.Codec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

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
class RPCCodec implements Codec<MessageLite> {
    static final RPCCodec DEFAULT = new RPCCodec();

    private RPCCodec() {
    }

    @Override
    public MessageLite decode(byte[] value) {
        MessageLite message = null;
        MsgType type = MsgType.forNumber(value[0]);

        if (null != type) {
            try {
                InputStream in = new ByteArrayInputStream(value, 1, value.length - 1);
                switch (type) {
                    case MsgSearchReq:
                        message = SearchRequest.parseFrom(in);
                        break;
                    case MsgSearchRsp:
                        message = SearchResponse.parseFrom(in);
                        break;
                    case MsgErrorRsp:
                        message = ErrResponse.parseFrom(in);
                        break;
                }
            } catch (Throwable e) {
                log.error("decode failed", e);
            }
        }

        return message;
    }

    @Override
    public ByteBuf encode(ByteBufAllocator allocator, MessageLite value) {
        ByteBuf buf = allocator.buffer();
        if (value instanceof SearchRequest) {
            buf.writeByte(MsgType.MsgSearchReq.getNumber());
            buf.writeBytes(value.toByteArray());
        } else if (value instanceof SearchResponse) {
            buf.writeByte(MsgType.MsgSearchRsp.getNumber());
            buf.writeBytes(value.toByteArray());
        }

        return buf;
    }
}
