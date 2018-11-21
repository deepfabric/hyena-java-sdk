package io.aicloud.sdk.hyena;

import java.util.concurrent.TimeUnit;

/**
 * Description:
 * <pre>
 * Date: 2018-10-31
 * Time: 11:10
 * </pre>
 *
 * @author fagongzi
 */
public class Builder {
    private String[] hyenaAddresses;
    private Options options = new Options();

    public Builder(String... hyenaAddresses) {
        this.hyenaAddresses = hyenaAddresses;
    }

    public Builder executors(int executors) {
        options.setExecutors(executors);
        return this;
    }

    public Builder ioExecutors(int ioExecutors) {
        options.setIoExecutors(ioExecutors);
        return this;
    }

    public Builder dim(int dim) {
        options.setDim(dim);
        return this;
    }

    public Builder kafka(String brokers, String topic) {
        options.setBrokers(brokers);
        options.setTopic(topic);
        return this;
    }

    public Builder timeout(long timeout, TimeUnit unit) {
        options.setTimeout(timeout);
        options.setTimeoutUnit(unit);
        return this;
    }

    public Client build() throws Exception {
        return new MQBasedClient(options, hyenaAddresses);
    }
}
