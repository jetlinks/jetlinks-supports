package org.jetlinks.supports.cache;

import reactor.core.Disposable;

import java.io.Serializable;

/**
 * 基于时间的文件数据存储工具类. 可将数据存储到本地磁盘,并可根据基准时间来获取对应的数据.
 *
 * <pre>{@code
 *
 * TimeBaseFileStore<Val> store = TimeBaseFileStore.open("./data/cache");
 *
 * store.set("test-data",data.getId(),time,data);
 *
 * }</pre>
 * <p>
 * 数据将在合适的时机进行持久化,在结束程序或者关闭存储时,应该调用{@link TimeBaseFileStore#dispose()}来提交文件,否则
 * 可能导致数据丢失或者损坏
 *
 * @param <T>
 * @author zhouhao
 * @since 1.2
 */
public interface TimeBaseFileStore<T extends Serializable> extends Disposable {

    /**
     * 获取在基准时间及之前的一条数据,如果数据不存在则返回null.
     *
     * @param name     存储名称,注意: 这个值应该相对固定.
     * @param key      存储key
     * @param baseTime 基准时间
     * @return 数据值
     */
    T get(String name, String key, long baseTime);

    /**
     * 设置基准时间的值
     *
     * @param name     存储名称
     * @param key      key
     * @param baseTime 基准时间
     * @param value    值
     */
    void set(String name, String key, long baseTime, T value);

    /**
     * 移除key对应的所有数据
     *
     * @param name 存储名称
     * @param key  key
     */
    void remove(String name, String key);

    /**
     * 移除指定存储名称对应的全部数据
     *
     * @param name 名称
     */
    void removeAll(String name);

    /**
     * 删除全部数据
     */
    void clear();

    /**
     * 使用指定的文件创建存储
     *
     * @param file 文件名
     * @param <T>  数据类型
     * @return TimeBaseFileStore
     */
    static <T extends Serializable> TimeBaseFileStore<T> open(String file) {
        return new MVStoreTimeBaseFileStore<>(file);
    }

    /**
     * 使用指定的文件以及最大缓存数量创建存储
     *
     * @param file                文件名
     * @param maxCacheSizeEachKey 每个key的最大缓存数量
     * @param <T>                 t
     * @return TimeBaseFileStore
     */
    static <T extends Serializable> TimeBaseFileStore<T> open(String file, int maxCacheSizeEachKey) {
        MVStoreTimeBaseFileStore<T> fileStore = new MVStoreTimeBaseFileStore<>(file);
        fileStore.setMaxStoreSizeEachKey(maxCacheSizeEachKey);
        return fileStore;
    }
}
