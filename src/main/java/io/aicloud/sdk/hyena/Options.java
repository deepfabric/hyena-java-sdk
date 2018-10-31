package io.aicloud.sdk.hyena;

import lombok.Getter;
import lombok.Setter;

/**
 * Description:
 * <pre>
 * Date: 2018-10-31
 * Time: 11:17
 * </pre>
 *
 * @author fagongzi
 */
@Getter
@Setter
class Options {
    private int executors = Runtime.getRuntime().availableProcessors();
    private int ioExecutors = 1;
}
