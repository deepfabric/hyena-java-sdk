package io.aicloud.sdk.hyena;

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

    public Client build() {
        return new MQBasedClient(options, hyenaAddresses);
    }
}
