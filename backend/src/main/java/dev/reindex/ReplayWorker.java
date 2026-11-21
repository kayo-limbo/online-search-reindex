package dev.reindex;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Duration;

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
            try (var fail = query("UPDATE replay_job SET status='failed', failure=? WHERE target=? AND status='running'",
                    failure.toString(), target)) { fail.executeUpdate(); }
            throw failure;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("Expected target collection");
        try (var db = DriverManager.getConnection(required("JDBC_URL"), required("PGUSER"), required("PGPASSWORD"))) {
            new ReplayWorker(db).run(args[0]);
        }
    }
}
