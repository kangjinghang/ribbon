package com.netflix.loadbalancer;

/**
 * strategy for {@link com.netflix.loadbalancer.DynamicServerListLoadBalancer} to use for different ways
 * of doing dynamic server list updates.
 *
 * @author David Liu
 */
public interface ServerListUpdater {

    /**
     * an interface for the updateAction that actually executes a server list update
     */
    public interface UpdateAction { // 内部接口，实现对 ServerList 的更新操作
        void doUpdate();
    }


    /**
     * start the serverList updater with the given update action
     * This call should be idempotent.
     *
     * @param updateAction
     */
    void start(UpdateAction updateAction); // 启动服务更新器

    /**
     * stop the serverList updater. This call should be idempotent
     */
    void stop(); // 停止服务更新器

    /**
     * @return the last update timestamp as a {@link java.util.Date} string
     */
    String getLastUpdate();  // 返回最近的更新时间戳

    /**
     * @return the number of ms that has elapsed since last update
     */
    long getDurationSinceLastUpdateMs();  // 返回上一次更新到现在的时间间隔，单位为 ms

    /**
     * @return the number of update cycles missed, if valid
     */
    int getNumberMissedCycles(); // 返回错过的更新周期数

    /**
     * @return the number of threads used, if vaid
     */
    int getCoreThreads(); // 返回核心线程数
}
