package dev.reindex;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class ReplayWorker {
    private final Connection db;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final ObjectMapper json = new ObjectMapper();
    private final String endpoint = required("TYPESENSE_URL");
    private final String key = required("TYPESENSE_API_KEY");

    private ReplayWorker(Connection db) { this.db = db; }

    private static String required(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) throw new IllegalArgumentException("Missing " + name);
        return value;
    }

    private PreparedStatement query(String sql, Object... args) throws SQLException {
        var statement = db.prepareStatement(sql);
        for (int index = 0; index < args.length; index++) statement.setObject(index + 1, args[index]);
        return statement;
    }

    private static String segment(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private void apply(String target, String operation, String id, String payload) throws Exception {
        String path = endpoint + "/collections/" + segment(target) + "/documents";
        var request = HttpRequest.newBuilder().timeout(Duration.ofSeconds(10)).header("X-TYPESENSE-API-KEY", key);
        switch (operation) {
            case "delete" -> request.uri(URI.create(path + "/" + segment(id))).DELETE();
            case "upsert" -> request.uri(URI.create(path + "?action=upsert")).header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(payload));
            default -> throw new IllegalStateException("Unsupported event " + operation);
        }
        var response = http.send(request.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() / 100 != 2 || !id.equals(json.readTree(response.body()).path("id").asText()))
            throw new IllegalStateException("Unconfirmed " + operation + " for " + id + ", HTTP " + response.statusCode());
    }

    private boolean cutover(String target, String source, long confirmed) throws SQLException {
        db.setAutoCommit(false);
        try (var guard = query("SELECT try_cutover(?, ?, ?)", source, target, confirmed);
             var result = guard.executeQuery()) {
            result.next();
            boolean switched = result.getBoolean(1);
            if (switched) {
                try (var init = query("INSERT INTO active_sync(target, confirmed_seq) VALUES (?, ?)", target, confirmed)) {
                    init.executeUpdate();
                }
                try (var finish = query("UPDATE replay_job SET status='complete' WHERE target=? AND status='running'", target)) {
                    if (finish.executeUpdate() != 1) throw new SQLException("Lost replay job");
                }
            }
            db.commit();
            return switched;
        } catch (SQLException failure) {
            db.rollback();
            throw failure;
        } finally {
            db.setAutoCommit(true);
        }
    }

    private void rebuild(String target) throws Exception {
        db.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        db.setAutoCommit(false);
        String source;
        long boundary;
        try {
            try (var select = query("SELECT active, committed_seq FROM index_control"); var row = select.executeQuery()) {
                row.next();
                source = row.getString(1);
                boundary = row.getLong(2);
            }
            try (var job = query("INSERT INTO replay_job(target, source, confirmed_seq, status) VALUES (?, ?, 0, 'running')",
                    target, source); var snapshot = query("CREATE TEMP TABLE rebuild_snapshot AS SELECT id, to_jsonb(documents)::text AS payload FROM documents");
                 var index = query("CREATE UNIQUE INDEX ON rebuild_snapshot(id)")) {
                job.executeUpdate();
                snapshot.execute();
                index.execute();
            }
            db.commit();
        } catch (SQLException failure) {
            db.rollback();
            throw failure;
        } finally {
            db.setAutoCommit(true);
            db.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        }
        try {
            importSnapshot(target);
            try (var save = query("UPDATE replay_job SET confirmed_seq=? WHERE target=?", boundary, target)) {
                save.executeUpdate();
            }
        } catch (Exception failure) {
            recordFailure(target, failure);
            throw failure;
        }
        replay(target, source, boundary);
    }

    private String post(String path, String type, String body) throws Exception {
        var request = HttpRequest.newBuilder(URI.create(endpoint + path)).timeout(Duration.ofSeconds(30))
                .header("X-TYPESENSE-API-KEY", key).header("Content-Type", type)
                .POST(HttpRequest.BodyPublishers.ofString(body)).build();
        var response = http.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() / 100 != 2) throw new IllegalStateException(path + ": HTTP " + response.statusCode());
        return response.body();
    }

    private void importSnapshot(String target) throws Exception {
        var schema = Map.of("name", target, "fields", List.of(Map.of("name", "title", "type", "string"),
                Map.of("name", "body", "type", "string"), Map.of("name", "category", "type", "string", "facet", true)));
        post("/collections", "application/json", json.writeValueAsString(schema));
        String cursor = null;
        while (true) {
            var documents = new ArrayList<String>();
            try (var page = query("SELECT id, payload FROM rebuild_snapshot WHERE (?::text IS NULL OR id > ?) ORDER BY id LIMIT 128",
                    cursor, cursor); var rows = page.executeQuery()) {
                while (rows.next()) {
                    cursor = rows.getString(1);
                    documents.add(rows.getString(2));
                }
            }
            if (documents.isEmpty()) return;
            var results = post("/collections/" + segment(target) + "/documents/import?action=create",
                    "text/plain", String.join("\n", documents)).lines().toList();
            if (results.size() != documents.size()) throw new IllegalStateException("Incomplete snapshot batch");
            for (String result : results) {
                var entry = json.readTree(result);
                if (!entry.path("success").asBoolean(false))
                    throw new IllegalStateException("Snapshot import failed: " + entry.path("error").asText());
            }
        }
    }

    private void run(String target) throws Exception {
        String source;
        long confirmed;
        try (var claim = query("UPDATE replay_job SET status='running' WHERE target=? AND status='ready' "
                + "AND source=(SELECT active FROM index_control) RETURNING source, confirmed_seq", target);
             var job = claim.executeQuery()) {
            if (!job.next()) throw new IllegalStateException("No ready replay job for " + target);
            source = job.getString(1);
            confirmed = job.getLong(2);
        }
        replay(target, source, confirmed);
    }

    private void recordFailure(String target, Exception failure) throws SQLException {
        try (var fail = query("UPDATE replay_job SET status='failed', failure=? WHERE target=? AND status='running'",
                failure.toString(), target)) { fail.executeUpdate(); }
    }

    private void replay(String target, String source, long confirmed) throws Exception {
        try {
            for (int batch = 0; batch < 1000; batch++) {
                try (var read = query("SELECT seq, operation, document_id, payload FROM document_changes "
                        + "WHERE seq > ? ORDER BY seq LIMIT 128", confirmed); var events = read.executeQuery()) {
                    while (events.next()) {
                        long seq = events.getLong(1);
                        if (seq != confirmed + 1) throw new IllegalStateException("Missing event after " + confirmed);
                        apply(target, events.getString(2), events.getString(3), events.getString(4));
                        try (var save = query("UPDATE replay_job SET confirmed_seq=? WHERE target=? AND confirmed_seq=? "
                                + "AND status='running'", seq, target, confirmed)) {
                            if (save.executeUpdate() != 1) throw new SQLException("Lost replay checkpoint");
                        }
                        confirmed = seq;
                    }
                }
                if (cutover(target, source, confirmed)) return;
            }
            throw new IllegalStateException("Replay did not catch up within batch limit");
        } catch (Exception failure) {
            recordFailure(target, failure);
            throw failure;
        }
    }

    private boolean syncNext() throws Exception {
        String target;
        long seq;
        String operation;
        String id;
        String payload;
        try (var claim = query("""
                UPDATE active_sync AS progress SET pending_seq=event.seq
                FROM index_control AS pointer, document_changes AS event
                WHERE progress.target=pointer.active AND progress.pending_seq IS NULL
                  AND event.seq=progress.confirmed_seq+1
                RETURNING progress.target,event.seq,event.operation,event.document_id,event.payload
                """); var row = claim.executeQuery()) {
            if (!row.next()) return false;
            target = row.getString(1);
            seq = row.getLong(2);
            operation = row.getString(3);
            id = row.getString(4);
            payload = row.getString(5);
        }
        try {
            apply(target, operation, id, payload);
            try (var save = query("UPDATE active_sync SET confirmed_seq=?,pending_seq=NULL WHERE target=? AND pending_seq=?",
                    seq, target, seq)) {
                if (save.executeUpdate() != 1) throw new SQLException("Lost active checkpoint");
            }
        } catch (Exception failure) {
            try (var mark = query("UPDATE active_sync SET failure=? WHERE target=? AND pending_seq=?",
                    failure.toString(), target, seq)) { mark.executeUpdate(); }
            throw failure;
        }
        return true;
    }

    public static void main(String[] args) throws Exception {
        boolean rebuild = args.length == 2 && args[0].equals("rebuild");
        if (!rebuild && args.length != 1) throw new IllegalArgumentException("Expected [rebuild] target");
        try (var db = DriverManager.getConnection(required("JDBC_URL"), required("PGUSER"), required("PGPASSWORD"))) {
            var worker = new ReplayWorker(db);
            if (args.length == 1 && args[0].equals("sync")) {
                while (!Thread.currentThread().isInterrupted()) {
                    if (!worker.syncNext()) Thread.sleep(200);
                }
            } else if (rebuild) worker.rebuild(args[1]);
            else worker.run(args[0]);
        }
    }
}
