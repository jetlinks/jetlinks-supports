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


    private String publicAddress = "127.0.0.1";
    private int port = ThreadLocalRandom.current().nextInt(10000, 30000);
    private int publicPort = port;


    public static RSocketAddress of(String address, int port) {
        return of(address, port, port);
    }

    public static RSocketAddress of(int port) {
        return of("127.0.0.1", port, port);
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