package io.aicloud.sdk.hyena;

import io.aicloud.sdk.hyena.pb.InsertRequest;
import io.aicloud.sdk.hyena.pb.SearchRequest;
import io.aicloud.sdk.hyena.pb.UpdateRequest;

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
    long insert(InsertRequest request) throws Exception;

    long update(UpdateRequest request) throws Exception;

    Future search(Float ...xqs) throws Exception;
}
