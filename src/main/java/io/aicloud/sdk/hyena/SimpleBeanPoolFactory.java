package io.aicloud.sdk.hyena;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.PooledSoftReference;
import org.apache.commons.pool2.impl.SoftReferenceObjectPool;

import java.lang.ref.SoftReference;
import java.util.function.Supplier;

/**
 * Description:
 * <pre>
 * Date: 2018-10-31
 * Time: 9:43
 * </pre>
 *
 * @author fagongzi
 */
class SimpleBeanPoolFactory {
    private SimpleBeanPoolFactory() {

    }

    static <T> ObjectPool<T> create(Supplier<T> creator) {
        return new SoftReferenceObjectPool<>(new SoftReferenceFactory<>(creator));
    }

    private static class SoftReferenceFactory<T> extends BasePooledObjectFactory<T> {
        private Supplier<T> creator;

        private SoftReferenceFactory(Supplier<T> creator) {
            this.creator = creator;
        }

        @Override
        public T create() throws Exception {
            return creator.get();
        }

        @Override
        public PooledObject<T> wrap(T obj) {
            return new PooledSoftReference<>(new SoftReference<>(obj));
        }
    }
}
