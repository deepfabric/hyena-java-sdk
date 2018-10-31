package io.aicloud.sdk.hyena;

import io.aicloud.sdk.hyena.pb.InsertRequest;
import io.aicloud.sdk.hyena.pb.SearchRequest;
import io.aicloud.sdk.hyena.pb.SearchResponse;
import io.aicloud.sdk.hyena.pb.UpdateRequest;

/**
 * Description:
 * <pre>
 * Date: 2018-10-31
 * Time: 11:10
 * </pre>
 *
 * @author fagongzi
 */
class MQBasedClient implements Client {
    private String[] hyenaAddresses;
    private Options options;

    private Watcher watcher;
    private Router router;

    MQBasedClient(Options options, String... hyenaAddresses) {
        this.options = options;
        this.hyenaAddresses = hyenaAddresses;
        this.router = new Router(options.getExecutors(), options.getIoExecutors());
        this.watcher = new Watcher(router, hyenaAddresses);
    }

    @Override
    public void insert(InsertRequest request) throws Exception {

    }

    @Override
    public void update(UpdateRequest request) throws Exception {

    }

    @Override
    public SearchResponse search(SearchRequest request) throws Exception {
        return null;
    }
}
