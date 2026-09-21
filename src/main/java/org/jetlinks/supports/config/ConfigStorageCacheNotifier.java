package org.jetlinks.supports.config;

import reactor.core.Disposable;

import java.util.function.Consumer;

/**
 * 配置存储本地缓存失效通知能力。
 *
 * @since 1.3.2
 */
public interface ConfigStorageCacheNotifier {

    /**
     * 监听本地缓存失效通知。实现类应同时转发本节点和集群节点产生的通知。
     *
     * @param listener 通知监听器
     * @return 用于取消监听的句柄
     */
    Disposable listenCacheNotify(Consumer<CacheNotify> listener);
}
