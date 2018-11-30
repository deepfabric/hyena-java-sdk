package io.aicloud.sdk.hyena;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * Description:
 * <pre>
 * Date: 2018-11-30
 * Time: 19:07
 * </pre>
 *
 * @author fagongzi
 */
@Setter
@Getter
public class SearchResult {
    private List<Long> dbs;
    private List<Float> distances;
    private List<Long> ids;
}
