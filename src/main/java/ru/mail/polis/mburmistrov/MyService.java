package ru.mail.polis.mburmistrov;

import com.sun.net.httpserver.HttpServer;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;

public class MyService implements KVService {
    private final static String PREFIX = "id=";

    @NotNull
    private final HttpServer server;
    @NotNull
    private final MyDAO dao;

    private static String extractId(@NotNull final String query) {
        if (!query.startsWith(PREFIX)) {
            throw new IllegalArgumentException("Invalid string");
        }
        final String id = query.substring(PREFIX.length());
        if (id.isEmpty()) {
            throw new IllegalArgumentException("Id is not specified");
        }

        return query.substring(PREFIX.length());
    }

    public MyService(
            int port,
            @NotNull final MyDAO dao) throws IOException{
        this.server =
                HttpServer.create(
                    new InetSocketAddress(port),
                    0);

        this.dao = dao;

        this.server.createContext(
                "/v0/status",
                http -> {
                        final String response = "ONLINE";
                        http.sendResponseHeaders(200, response.length());
                        http.getResponseBody().write(response.getBytes());
                        http.close();
                });

        this.server.createContext(
                "/v0/entity",
                http -> {
                    String id = null;
                    try {
                        id = extractId(http.getRequestURI().getQuery());
                    } catch (Exception e) {
                        http.sendResponseHeaders(400, 0);
                        http.close();
                        return;
                    }

                    switch (http.getRequestMethod()) {
                        case "GET":
                            try {
                                final byte[] getValue = dao.get(id);
                                http.sendResponseHeaders(200, getValue.length);
                                http.getResponseBody().write(getValue);
                            } catch (IOException e) {
                                http.sendResponseHeaders(404, 0);
                                http.close();
                            }
                            break;

                        case "DELETE":
                            dao.delete(id);
                            http.sendResponseHeaders(202, 0);
                            break;

                        case "PUT":
                            try {
                                final int contentLength =
                                        Integer.valueOf(
                                                http.getRequestHeaders().getFirst("Content-Length"));
                                final byte[] putValue = new byte[contentLength];
                                if (http.getRequestBody().read(putValue) != putValue.length && putValue.length != 0) {
                                    throw new IOException("Unable to read data in one take");
                                }
                                dao.upsert(id, putValue);
                                http.sendResponseHeaders(201, 0);
                            } catch (IllegalArgumentException e) {
                                http.sendResponseHeaders(400, 0);
                            } catch (NoSuchElementException e) {
                                http.sendResponseHeaders(404, 0);
                            }
                            break;
                        default:
                            http.sendResponseHeaders(405, 0);
                    }

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
