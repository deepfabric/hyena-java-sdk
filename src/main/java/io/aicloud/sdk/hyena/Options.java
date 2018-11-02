package io.aicloud.sdk.hyena;

import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.TimeUnit;

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
    private int dim = 512;
    private int executors = Runtime.getRuntime().availableProcessors();
    private int ioExecutors = 1;
    private int timeout = 200;
    private TimeUnit timeoutUnit = TimeUnit.MILLISECONDS;

    private String brokers;
    private String topic;

    @Override
    public String toString() {
        return "\ndim: " + dim + "\n"
                + "executors: " + executors + "\n"
                + "io executors: " + ioExecutors + "\n"
                + "timeout(" + timeoutUnit + "): " + timeout;
    }
}
