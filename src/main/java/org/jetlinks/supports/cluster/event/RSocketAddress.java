package org.jetlinks.supports.cluster.event;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.concurrent.ThreadLocalRandom;

@Getter
@Setter
@AllArgsConstructor(staticName = "of")
@NoArgsConstructor
public class RSocketAddress implements Serializable {
    private static final long serialVersionUID = -6849794470754667710L;
    /**
     * 绑定端口
     */
    private int port = ThreadLocalRandom.current().nextInt(10000, 30000);
    /**
     * 对外访问的公共地址
     */
    private String publicAddress = "127.0.0.1";

    /**
     * 对外访问的公共端口
     */
    private int publicPort = port;


    public static RSocketAddress of(String address, int port) {
        return of(port, address, port);
    }

    public static RSocketAddress of(int port) {
        return of(port, "127.0.0.1", port);
    }

    @Override
    public String toString() {
        return "{" +
                "publicAddress='" + publicAddress + '\'' +
                ", port=" + port +
                ", publicPort=" + publicPort +
                '}';
    }
}