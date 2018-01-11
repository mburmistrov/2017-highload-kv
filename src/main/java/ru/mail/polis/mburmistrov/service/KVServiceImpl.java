package ru.mail.polis.mburmistrov.service;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.KVService;
import ru.mail.polis.mburmistrov.http.HttpMethod;
import ru.mail.polis.mburmistrov.http.ParseQuery;
import ru.mail.polis.mburmistrov.http.QueryParams;
import ru.mail.polis.mburmistrov.http.Response;
import ru.mail.polis.mburmistrov.storage.Storage;
import ru.mail.polis.mburmistrov.util.DataReader;
import ru.mail.polis.mburmistrov.util.GetServers;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.*;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;

import static ru.mail.polis.mburmistrov.http.HttpMethod.*;
import static ru.mail.polis.mburmistrov.http.Response.*;

public class KVServiceImpl implements KVService {

    private static final String URL_STATUS = "/v0/status";

    private static final String URL_INNER = "/v0/inner";

    private static final String URL_ENTITY = "/v0/entity";

    private static final String URL_SERVER = "http://localhost";

    private static final String METHOD_IS_NOT_ALLOWED = "Method is not allowed";

    @NotNull
    private final HttpServer server;

    @NotNull
    private final Storage dao;

    @NotNull
    private final List<String> topology;

    @NotNull
    private final CompletionService<Response> completionService;

    public KVServiceImpl(int port,
                         @NotNull Storage dao,
                         @NotNull Set<String> topology) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = dao;
        this.topology = new ArrayList<>(topology);

        Executor executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        this.completionService = new ExecutorCompletionService<>(executor);

        server.createContext(URL_STATUS, this::processStatus);
        server.createContext(URL_INNER, this::processInner);
        server.createContext(URL_ENTITY, this::processEntity);
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
    }

    private void processStatus(@NotNull HttpExchange http) throws IOException {
        sendResponse(http, new Response(OK));
    }

    private void processInner(@NotNull HttpExchange http) throws IOException {
        try {
            QueryParams params = ParseQuery.parseQuery(http.getRequestURI().getQuery(), topology);

            Response response;
            switch (HttpMethod.valueOf(http.getRequestMethod())) {

                case PUT:
                    final byte[] data = DataReader.readData(http.getRequestBody());
                    response = innerPut(params, data);
                    break;

                case GET:
                    response = innerGet(params);
                    break;

                case DELETE:
                    response = innerDelete(params);
                    break;

                default:
                    response = new Response(NOT_ALLOWED, METHOD_IS_NOT_ALLOWED);
                    break;
            }
            sendResponse(http, response);
        } catch (IllegalArgumentException e) {
            sendResponse(http, new Response(BAD_REQUEST, e.getMessage()));
        }
    }

    private Response innerGet(@NotNull QueryParams params) throws IOException {
        try {
            String id = params.getId();
            final byte[] getValue = dao.get(id);

            return new Response(OK, getValue);
        } catch (IllegalArgumentException e) {
            return new Response(BAD_REQUEST, e.getMessage());
        } catch (NoSuchElementException e) {
            return new Response(NOT_FOUND, e.getMessage());
        }
    }

    private Response innerPut(@NotNull QueryParams params,
                              @NotNull byte[] data) {
        try {
            String id = params.getId();
            dao.upsert(id, data);

            return new Response(CREATED);
        } catch (IOException | IllegalArgumentException e) {
            return new Response(BAD_REQUEST, e.getMessage());
        }
    }

    private Response innerDelete(@NotNull QueryParams params) throws IOException {
        try {
            String id = params.getId();
            dao.delete(id);
            return new Response(ACCEPTED);
        } catch (IllegalArgumentException e) {
            return new Response(BAD_REQUEST, e.getMessage());
        }
    }

    private void processEntity(@NotNull HttpExchange http) throws IOException {
        try {
            QueryParams params = ParseQuery.parseQuery(http.getRequestURI().getQuery(), topology);

            Response response;
            switch (HttpMethod.valueOf(http.getRequestMethod())) {

                case PUT:
                    final byte[] data = DataReader.readData(http.getRequestBody());
                    response = processEntityPut(params, data);
                    break;

                case GET:
                    response = processEntityGet(params);
                    break;

                case DELETE:
                    response = processEntityDelete(params);
                    break;

                default:
                    response = new Response(NOT_ALLOWED, METHOD_IS_NOT_ALLOWED);
                    break;

            }

            sendResponse(http, response);
        } catch (IllegalArgumentException e) {
            sendResponse(http, new Response(BAD_REQUEST, e.getMessage()));
        }
    }

    private Response processEntityGet(@NotNull QueryParams params) throws IOException {
        try {
            String id = params.getId();
            List<String> nodes = GetServers.getNodesById(id, params.getFrom(), topology);
            executeFutures(GET, params, nodes, null);

            int success = 0;
            int notPresent = 0;
            byte[] value = null;
            for (int i = 0; i < params.getFrom(); i++) {
                try {
                    Response resp = completionService.take().get();

                    if (resp.getCode() == OK) {
                        success++;

                        value = resp.getData();
                    } else if (resp.getCode() == NOT_FOUND) {
                        notPresent++;
                    }
                } catch (Exception e) {
                    return new Response(SERVER_ERROR);
                }
            }

            boolean presentLocally = innerGet(params).getCode() == OK;

            if (success > 0 && notPresent == 1 && !presentLocally) {
                innerPut(params, value);

                notPresent--;

                success++;
            }

            if (success + notPresent < params.getAck()) {
                return new Response(NOT_ENOUGH_REPLICAS);
            } else if (success < params.getAck()) {
                return new Response(NOT_FOUND);
            } else {
                return new Response(OK, value);
            }
        } catch (IllegalArgumentException e) {
            return new Response(BAD_REQUEST, e.getMessage());
        } catch (NoSuchElementException e) {
            return new Response(NOT_FOUND, e.getMessage());
        }
    }

    private Response processEntityPut(@NotNull QueryParams params,
                                      @Nullable byte[] data) {
        try {
            List<String> nodes = GetServers.getNodesById(params.getId(), params.getFrom(), topology);
            executeFutures(PUT, params, nodes, data);

            int success = 0;

            for (int i = 0; i < params.getFrom() && success < params.getAck(); i++) {
                try {
                    Response resp = completionService.take().get();

                    if (resp.getCode() == CREATED) {
                        success++;
                    }
                } catch (Exception e) {
                    return new Response(SERVER_ERROR);
                }
            }

            if (success < params.getAck()) {
                return new Response(NOT_ENOUGH_REPLICAS);
            } else {
                return new Response(CREATED);
            }
        } catch (IllegalArgumentException e) {
            return new Response(BAD_REQUEST, e.getMessage());
        } catch (NoSuchElementException e) {
            return new Response(NOT_FOUND, e.getMessage());
        }
    }

    private Response processEntityDelete(@NotNull QueryParams params) {
        try {
            List<String> nodes = GetServers.getNodesById(params.getId(), params.getFrom(), topology);
            executeFutures(DELETE, params, nodes, null);

            int success = 0;

            for (int i = 0; i < params.getFrom() && success < params.getAck(); i++) {
                try {
                    Response resp = completionService.take().get();
                    if (resp.getCode() == ACCEPTED) {
                        success++;
                    }
                } catch (Exception e) {
                    return new Response(SERVER_ERROR);
                }
            }

            if (success < params.getAck()) {
                return new Response(NOT_ENOUGH_REPLICAS);
            } else {
                return new Response(ACCEPTED);
            }
        } catch (IllegalArgumentException e) {
            return new Response(BAD_REQUEST, e.getMessage());
        } catch (NoSuchElementException e) {
            return new Response(NOT_FOUND, e.getMessage());
        }
    }

    private void executeFutures(@NotNull HttpMethod method,
                                @NotNull QueryParams params,
                                @NotNull List<String> nodes,
                                @Nullable byte[] data) {
        String self = URL_SERVER + ":" + server.getAddress().getPort();
        for (String node : nodes) {
            if (node.equals(self)) {
                switch (method) {

                    case PUT:
                        completionService.submit(() -> innerPut(params, data));
                        break;

                    case GET:
                        completionService.submit(() -> innerGet(params));
                        break;

                    case DELETE:
                        completionService.submit(() -> innerDelete(params));
                        break;

                    default:
                        throw new IllegalArgumentException(METHOD_IS_NOT_ALLOWED);
                }
            } else {
                completionService.submit(() -> makeRequest(method, node + URL_INNER, "?id=" + params.getId(), data));
            }
        }
    }

    private void sendResponse(@NotNull HttpExchange http,
                              @NotNull Response response) throws IOException {
        if (response.hasData()) {
            http.sendResponseHeaders(response.getCode(), response.getData().length);
            http.getResponseBody().write(response.getData());
        } else {
            http.sendResponseHeaders(response.getCode(), 0);
        }
        http.close();
    }

    private Response makeRequest(@NotNull HttpMethod method,
                                 @NotNull String link,
                                 @NotNull String params,
                                 @Nullable byte[] data) {
        HttpURLConnection connection = null;
        try {
            URL url = new URL(link + params);

            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod(method.toString());
            connection.setDoOutput(method == PUT);
            connection.connect();

            if (method == PUT) {
                connection.getOutputStream().write(data);
                connection.getOutputStream().flush();
                connection.getOutputStream().close();
            }

            int code = connection.getResponseCode();
            if (method == GET && code == OK) {
                InputStream dataStream = connection.getInputStream();
                byte[] inputData = DataReader.readData(dataStream);

                return new Response(code, inputData);
            }

            return new Response(code);
        } catch (IOException e) {
            return new Response(SERVER_ERROR);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }
}
