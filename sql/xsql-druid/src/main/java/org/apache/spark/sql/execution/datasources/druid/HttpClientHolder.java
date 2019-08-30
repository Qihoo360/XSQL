package org.apache.spark.sql.execution.datasources.druid;

import com.google.common.base.Throwables;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.HttpClientConfig;
import com.metamx.http.client.HttpClientInit;

import java.io.Closeable;
import java.io.IOException;

public class HttpClientHolder implements Closeable {
    private final Lifecycle lifecycle;
    private final HttpClient client;

    public HttpClientHolder(Lifecycle lifecycle, HttpClient client) {
        this.lifecycle = lifecycle;
        this.client = client;
    }

    public static HttpClientHolder create() {
        final Lifecycle lifecycle = new Lifecycle();
        final HttpClient httpClient = HttpClientInit.createClient(
                HttpClientConfig.builder().build(),
                lifecycle
        );

        try {
            lifecycle.start();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }

        return new HttpClientHolder(lifecycle, httpClient);
    }

    public HttpClient get() {
        return client;
    }

    @Override
    public void close() throws IOException {
        lifecycle.stop();
    }
}
