package io.aicloud.sdk.hyena;

import io.aicloud.sdk.hyena.pb.SearchResponse;

import java.util.concurrent.TimeoutException;

/**
 * Description:
 * <pre>
 * Date: 2018-11-15
 * Time: 14:13
 * </pre>
 *
 * @author fagongzi
 */
public interface Future {
    SearchResponse get() throws TimeoutException, InterruptedException;
}
