package ru.mail.polis.mburmistrov;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import ru.mail.polis.KVService;

import java.io.IOException;
import java.net.InetSocketAddress;

public class MyService implements KVService {
    private final HttpServer server;

    public MyService(int port) throws IOException{
        this.server =
                HttpServer.create(
                    new InetSocketAddress(port),
                    0);

        this.server.createContext(
                "/v0/status",
                http -> {
                        final String response = "ONLINE";
                        http.sendResponseHeaders(200, response.length());
                        http.getResponseBody().write(response.getBytes());
                        http.close();
                });
    }

    @Override
    public void start() {
        this.server.start();
    }

    @Override
    public void stop() {
        this.server.stop(0);
    }
}
