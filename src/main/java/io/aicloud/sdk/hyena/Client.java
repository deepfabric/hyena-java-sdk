package io.aicloud.sdk.hyena;

import io.aicloud.sdk.hyena.pb.InsertRequest;
import io.aicloud.sdk.hyena.pb.SearchRequest;
import io.aicloud.sdk.hyena.pb.SearchResponse;
import io.aicloud.sdk.hyena.pb.UpdateRequest;

import java.util.concurrent.Future;

/**
 * Description:
 * <pre>
 * Date: 2018-10-31
 * Time: 11:08
 * </pre>
 *
 * @author fagongzi
 */
public interface Client {
    void insert(InsertRequest request) throws Exception;

    void update(UpdateRequest request) throws Exception;

    Future<SearchResponse> search(SearchRequest request) throws Exception;
}
