package org.jetlinks.supports.device.session;

import org.jetlinks.core.server.session.DeviceSession;
import org.jetlinks.core.utils.Reactors;
import reactor.core.publisher.Mono;

/**
 * 本地设备会话管理器
 *
 * @author zhouhao
 */
public class LocalDeviceSessionManager extends AbstractDeviceSessionManager{

    public static LocalDeviceSessionManager create(){
        return new LocalDeviceSessionManager();
    }

    /**
     * 当前集群节点ID
     *
     * @return 集群节点ID
     */
    @Override
    public String getCurrentServerId() {
        return "local";
    }

    /**
     * 初始化设备会话连接
     *
     * @param session 设备会话
     * @return
     */
    @Override
    protected Mono<Boolean> initSessionConnection(DeviceSession session) {
        return Reactors.ALWAYS_FALSE;
    }

    /**
     * 移除远程会话
     *
     * @param deviceId 设备ID
     * @return
     */
    @Override
    protected Mono<Long> removeRemoteSession(String deviceId) {
        return Reactors.ALWAYS_ZERO_LONG;
    }

    /**
     * 获取远程会话总数
     *
     * @return 远程会话总数
     */
    @Override
    protected Mono<Long> getRemoteTotalSessions() {
        return Reactors.ALWAYS_ZERO_LONG;
    }

    /**
     * 监测远程会话是否存活
     *
     * @param deviceId 设备ID
     * @return 是否存活
     */
    @Override
    protected Mono<Boolean> remoteSessionIsAlive(String deviceId) {
        return Reactors.ALWAYS_FALSE;
    }
}
