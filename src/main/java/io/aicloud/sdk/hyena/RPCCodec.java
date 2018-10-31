package io.aicloud.sdk.hyena;

import io.aicloud.sdk.hyena.pb.SearchRequest;
import io.aicloud.tools.netty.Codec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

/**
 * Description:
 * <pre>
 * Date: 2018-10-29
 * Time: 13:09
 * </pre>
 *
 * @author fagongzi
 */
class RPCCodec implements Codec<SearchRequest> {
    static final RPCCodec DEFAULT = new RPCCodec();

    private RPCCodec() {

    }

    @Override
    public SearchRequest decode(byte[] value) {
        return null;
    }

    @Override
    public ByteBuf encode(ByteBufAllocator allocator, SearchRequest value) {
        return null;
    }
}
