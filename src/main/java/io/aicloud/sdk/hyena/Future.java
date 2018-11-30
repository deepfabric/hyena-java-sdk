package io.aicloud.sdk.hyena;

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
    SearchResult get() throws TimeoutException, InterruptedException;
}
