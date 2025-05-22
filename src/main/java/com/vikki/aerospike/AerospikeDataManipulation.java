package com.vikki.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import javafx.application.Platform;
import javafx.scene.control.Label;
import javafx.scene.control.ProgressBar;
import javafx.scene.control.RadioButton;
import javafx.scene.control.TableView;

import java.util.*;
import java.util.concurrent.ExecutorService;

public class AerospikeDataManipulation {

    final AerospikeClient client;
    final ExecutorService executor;
    final Label statusLabel; // For individual operation status
    private final TableView<Map<String, Object>> dataTableView;
    private final ProgressBar progressBar;
    private final Label overallStatusLabel; // For overall query execution status
    private final RadioButton tableOutputRadio;
    private final RadioButton jsonOutputRadio;
    private SelectResultCallback selectResultCallback;

    public interface SelectResultCallback {
        void onResult(List<Map<String, Object>> results);
        void onError(String errorMessage);
    }

    public void setSelectResultCallback(SelectResultCallback callback) {
        this.selectResultCallback = callback;
    }

    public AerospikeDataManipulation(
            AerospikeClient client,
            ExecutorService executor,
            Label statusLabel,
            TableView<Map<String, Object>> dataTableView,
            ProgressBar progressBar,
            Label overallStatusLabel,
            RadioButton tableOutputRadio,
            RadioButton jsonOutputRadio
    ) {
        this.client = client;
        this.executor = executor;
        this.statusLabel = statusLabel;
        this.dataTableView = dataTableView;
        this.progressBar = progressBar;
        this.overallStatusLabel = overallStatusLabel;
        this.tableOutputRadio = tableOutputRadio;
        this.jsonOutputRadio = jsonOutputRadio;
    }

    public void executeAqlManipulation(String aql) {
        if (client != null && client.isConnected() && aql != null && !aql.trim().isEmpty()) {
            executor.submit(() -> {
                try {
                    String trimmedAql = aql.trim();
                    String command = trimmedAql.split("\\s+")[0].toUpperCase();

                    Platform.runLater(() -> {
                        progressBar.setVisible(true);
                        overallStatusLabel.setText("Executing: " + command + "...");
                    });

                    if (command.equals("SELECT")) {
                        executeAqlSelect(trimmedAql);
                    } else if (command.equals("INSERT")) {
                        executeAqlInsert(trimmedAql);
                    } else if (command.equals("DELETE")) {
                        executeAqlDelete(trimmedAql);
                    } else if (command.equals("UPDATE")) {
                        executeAqlUpdate(trimmedAql);
                    } else {
                        Platform.runLater(() -> {
                            overallStatusLabel.setText("Unsupported AQL command: " + command);
                            progressBar.setVisible(false);
                        });
                    }
                } catch (AerospikeException e) {
                    Platform.runLater(() -> {
                        overallStatusLabel.setText("AQL Execution Error: " + e.getMessage());
                        if (selectResultCallback != null) {
                            selectResultCallback.onError(e.getMessage());
                        }
                        progressBar.setVisible(false);
                    });
                    e.printStackTrace();
                } finally {
                    // Progress bar for non-SELECT commands is managed within each command's logic
                    if (!aql.trim().toUpperCase().startsWith("SELECT")) {
                        Platform.runLater(() -> progressBar.setVisible(false));
                    }
                }
            });
        } else if (client == null || !client.isConnected()) {
            Platform.runLater(() -> overallStatusLabel.setText("Not connected to Aerospike."));
        } else {
            Platform.runLater(() -> overallStatusLabel.setText("Empty AQL query."));
        }
    }

    private void executeAqlSelect(String aql) {
        executor.submit(() -> {
            List<Map<String, Object>> results = new ArrayList<>();
            Statement stmt = new Statement();
            String[] parts = aql.split("(?i)\\s+FROM\\s+");
            if (parts.length == 2) {
                String selectClause = parts[0].trim();
                String fromClause = parts[1].trim();
                String[] fromParts = fromClause.split("(?i)\\s+WHERE\\s+");
                String namespaceSet = fromParts[0].trim();
                String whereClause = null;
                if (fromParts.length == 2) {
                    whereClause = fromParts[1].trim();
                }

                String[] nsParts = namespaceSet.split("\\.");
                if (nsParts.length == 2) {
                    stmt.setNamespace(nsParts[0]);
                    stmt.setSetName(nsParts[1]);

                    if (selectClause.toUpperCase().equals("SELECT *")) {
                        // Fetch all bins
                        stmt.setBinNames(null); // Null means all bins
                    } else {
                        // Fetch specific bins
                        String[] bins = selectClause.substring(6).trim().split(",");
                        stmt.setBinNames(bins);
                    }

                    if (whereClause != null && !whereClause.isEmpty()) {
                        String[] whereCondition = whereClause.split("=");
                        if (whereCondition.length == 2) {
                            String binName = whereCondition[0].trim();
                            String value = whereCondition[1].trim();
                            if (value.startsWith("'") && value.endsWith("'")) {
                                stmt.setFilter(Filter.equal(binName, value.substring(1, value.length() - 1)));
                            } else if (value.matches("-?\\d+")) {
                                stmt.setFilter(Filter.equal(binName, Integer.parseInt(value)));
                            } else if (value.matches("-?\\d+(\\.\\d+)?")) {
                                stmt.setFilter(Filter.equal(binName, (long) Double.parseDouble(value)));
                            } else {
                                Platform.runLater(() -> {
                                    overallStatusLabel.setText("WHERE clause value must be a string, integer, or double.");
                                    if (selectResultCallback != null) {
                                        selectResultCallback.onError("WHERE clause value must be a string, integer, or double.");
                                    }
                                    progressBar.setVisible(false);
                                });
                                return;
                            }
                        } else {
                            Platform.runLater(() -> {
                                overallStatusLabel.setText("Invalid WHERE clause format.");
                                if (selectResultCallback != null) {
                                    selectResultCallback.onError("Invalid WHERE clause format.");
                                }
                                progressBar.setVisible(false);
                            });
                            return;
                        }
                    }

                    QueryPolicy queryPolicy = new QueryPolicy();
//                    queryPolicy.executeMode = com.aerospike.client.query.ExecuteMode.ALL;

                    try (RecordSet recordSet = client.query(queryPolicy, stmt)) {
                        recordSet.forEach(record -> {
                            Map<String, Object> recordData = new HashMap<>();
                            recordData.put("Key", record.key.userKey != null ? record.key.userKey.toString() : null);
                            recordData.put("Namespace", record.key.namespace);
                            recordData.put("Set", record.key.setName);
                            if (record.record != null) {
                                recordData.put("Generation", record.record.generation);
                                recordData.put("TTL", record.record.getTimeToLive());
                                if (record.record.bins != null) {
                                    recordData.putAll(record.record.bins);
                                }
                            }
                            results.add(recordData);
                        });
                        if (selectResultCallback != null) {
                            Platform.runLater(() -> {
                                selectResultCallback.onResult(results);
                                progressBar.setVisible(false);
                            });
                        }
                    } catch (AerospikeException e) {
                        Platform.runLater(() -> {
                            overallStatusLabel.setText("AQL Query Error: " + e.getMessage());
                            if (selectResultCallback != null) {
                                selectResultCallback.onError(e.getMessage());
                            }
                            progressBar.setVisible(false);
                        });
                        e.printStackTrace();
                    }
                } else {
                    Platform.runLater(() -> {
                        overallStatusLabel.setText("Invalid FROM clause format (namespace.set expected).");
                        if (selectResultCallback != null) {
                            selectResultCallback.onError("Invalid FROM clause format (namespace.set expected).");
                        }
                        progressBar.setVisible(false);
                    });
                }
            } else {
                Platform.runLater(() -> {
                    overallStatusLabel.setText("Invalid SELECT query format.");
                    if (selectResultCallback != null) {
                        selectResultCallback.onError("Invalid SELECT query format.");
                    }
                    progressBar.setVisible(false);
                });
            }
        });
    }

    private void executeAqlInsert(String aql) {
        if (aql.toUpperCase().startsWith("INSERT INTO")) {
            executor.submit(() -> {
                try {
                    // Split the query to extract the namespace, set, and values
                    String[] parts = aql.split("\\s+");
                    if (parts.length >= 4 && parts[2].contains(".")) {
                        String[] namespaceSet = parts[2].split("\\.");
                        if (namespaceSet.length == 2) {
                            String namespace = namespaceSet[0];
                            String set = namespaceSet[1];

                            // Extract the bin names
                            int openParenIndex = aql.indexOf('(');
                            int closeParenIndex = aql.indexOf(')', openParenIndex);
                            String binNamesStr = aql.substring(openParenIndex + 1, closeParenIndex).trim();
                            String[] binNames = binNamesStr.split(",");

                            // Extract the values
                            int valuesStart = aql.indexOf("VALUES (");
                            int valuesEnd = aql.indexOf(')', valuesStart);
                            String valuesStr = aql.substring(valuesStart + 8, valuesEnd).trim();
                            String[] values = valuesStr.split(",");

                            // Ensure the number of bin names matches the number of values
                            if (binNames.length != values.length) {
                                Platform.runLater(() -> overallStatusLabel.setText("Number of bin names does not match number of values."));
                                return;
                            }

                            // Create a map of bin names to values
                            Map<String, Object> bins = new HashMap<>();
                            for (int i = 0; i < binNames.length; i++) {
                                String binName = binNames[i].trim();
                                String valueStr = values[i].trim();
                                Object binValue = parseAqlValue(valueStr);
                                if (binValue == null) {
                                    Platform.runLater(() -> overallStatusLabel.setText("Error parsing value: " + valueStr));
                                    return;
                                }
                                bins.put(binName, binValue);
                            }

                            String name = (String) bins.getOrDefault("Pk", bins.get("KEY"));;
                            if (name == null || name.isEmpty()) {
                                name = UUID.randomUUID().toString(); // Generate a random UUID as the key
                            }
                            bins.put("PK",name);
                            // Create a unique key
                            Key aerospikeKey = new Key(namespace, set, name);

                            // Insert the record into Aerospike
                            try {
                                Bin[] aerospikeBins = bins.entrySet().stream()
                                        .filter(entry -> !entry.getKey().equalsIgnoreCase("KEY")) // Don't store 'KEY' bin itself
                                        .map(entry -> new Bin(entry.getKey(), entry.getValue()))
                                        .toArray(Bin[]::new);
                                client.put(null, aerospikeKey, aerospikeBins);
                                Platform.runLater(() -> overallStatusLabel.setText("Record inserted: " + namespace + "." + set + "." + aerospikeKey));
                            } catch (AerospikeException e) {
                                Platform.runLater(() -> overallStatusLabel.setText("Error inserting record: " + e.getMessage()));
                            }
                        } else {
                            Platform.runLater(() -> overallStatusLabel.setText("Invalid namespace.set format in INSERT."));
                        }
                    } else {
                        Platform.runLater(() -> overallStatusLabel.setText("Invalid INSERT INTO syntax."));
                    }
                } finally {
                    Platform.runLater(() -> progressBar.setVisible(false));
                }
            });
        } else {
            Platform.runLater(() -> {
                overallStatusLabel.setText("Unsupported INSERT syntax.");
                progressBar.setVisible(false);
            });
        }
    }



    private void executeAqlDelete(String aql) {
        if (aql.toUpperCase().startsWith("DELETE FROM")) {
            executor.submit(() -> {
                try{
                String[] parts = aql.split("\\s+");
                if (parts.length >= 3 && parts[2].contains(".")) {
                    String[] namespaceSet = parts[2].split("\\.");
                    if (namespaceSet.length == 2) {
                        String namespace = namespaceSet[0];
                        String set = namespaceSet[1];

                        int whereClauseIndex = aql.toUpperCase().indexOf("WHERE");
                        if (whereClauseIndex > 0) {
                            String condition = aql.substring(whereClauseIndex + 5).trim();
                            String[] conditionParts = condition.split("=");
                            if (conditionParts.length == 2) {
                                String binName = conditionParts[0].trim();
                                String valueStr = conditionParts[1].trim();
                                Object value = parseAqlValue(valueStr);
                                if (value instanceof String) {
                                    // Basic delete based on a single string equality condition
                                    scanAndDelete(namespace, set, binName, (String) value);
                                } else {
                                    Platform.runLater(() -> overallStatusLabel.setText("DELETE WHERE value must be a string for this basic implementation."));
                                }
                            } else {
                                Platform.runLater(() -> overallStatusLabel.setText("Invalid WHERE clause in DELETE."));
                            }
                        } else {
                            Platform.runLater(() -> overallStatusLabel.setText("DELETE requires a WHERE clause in this basic implementation."));
                        }
                    } else {
                        Platform.runLater(() -> overallStatusLabel.setText("Invalid namespace.set format in DELETE."));
                    }
                } else {
                    Platform.runLater(() -> overallStatusLabel.setText("Invalid DELETE FROM syntax."));
                } }finally {
                    Platform.runLater(() -> progressBar.setVisible(false));
                }
            });
        } else {
            Platform.runLater(() -> {
                overallStatusLabel.setText("Unsupported DELETE syntax.");
                progressBar.setVisible(false);
            });
        }
    }

    private void executeAqlUpdate(String aql) {
        if (aql.toUpperCase().startsWith("UPDATE")) {
            executor.submit(() -> {
                try{
                String[] parts = aql.split("\\s+");
                if (parts.length >= 2 && parts[1].contains(".")) {
                    String[] namespaceSet = parts[1].split("\\.");
                    if (namespaceSet.length == 2) {
                        String namespace = namespaceSet[0];
                        String set = namespaceSet[1];

                        int setClauseIndex = aql.toUpperCase().indexOf("SET");
                        int whereClauseIndex = aql.toUpperCase().indexOf("WHERE");

                        if (setClauseIndex > 0 && whereClauseIndex > setClauseIndex) {
                            String setClause = aql.substring(setClauseIndex + 3, whereClauseIndex).trim();
                            String whereClause = aql.substring(whereClauseIndex + 5).trim();

                            Map<String, Object> updateBins = new HashMap<>();
                            String[] assignments = setClause.split(",");
                            for (String assignment : assignments) {
                                String[] kv = assignment.trim().split("=");
                                if (kv.length == 2) {
                                    String binName = kv[0].trim();
                                    String valueStr = kv[1].trim();
                                    Object binValue = parseAqlValue(valueStr);
                                    if (binValue != null) {
                                        updateBins.put(binName, binValue);
                                    } else {
                                        Platform.runLater(() -> overallStatusLabel.setText("Error parsing value in SET clause: " + valueStr));
                                        return;
                                    }
                                } else {
                                    Platform.runLater(() -> overallStatusLabel.setText("Invalid bin=value pair in SET clause: " + assignment));
                                    return;
                                }
                            }

                            String[] whereParts = whereClause.split("=");
                            if (whereParts.length == 2) {
                                String keyBin = whereParts[0].trim();
                                String keyValueStr = whereParts[1].trim();
                                Object keyValueObj = parseAqlValue(keyValueStr);
                                if (keyValueObj instanceof String) {
                                    String keyValue = (String) keyValueObj;
                                    if (!updateBins.isEmpty()) {
                                        Key key = new Key(namespace, set, keyValue);
                                        try {
                                            Bin[] aerospikeBins = updateBins.entrySet().stream()
                                                    .filter(entry -> !entry.getKey().equalsIgnoreCase("KEY")) // Don't store 'KEY' bin itself
                                                    .map(entry -> new Bin(entry.getKey(), entry.getValue()))
                                                    .toArray(Bin[]::new);
                                            client.put(null, key, aerospikeBins);
                                            Platform.runLater(() -> overallStatusLabel.setText("Record updated: " + namespace + "." + set + "." + keyValue));
                                        } catch (AerospikeException e) {
                                            Platform.runLater(() -> overallStatusLabel.setText("Error updating record: " + e.getMessage()));
                                        }
                                    } else {
                                        Platform.runLater(() -> overallStatusLabel.setText("No bins to update."));
                                    }
                                } else {
                                    Platform.runLater(() -> overallStatusLabel.setText("Key value in WHERE clause must be a string for this basic implementation."));
                                }
                            } else {
                                Platform.runLater(() -> overallStatusLabel.setText("Invalid WHERE clause in UPDATE."));
                            }
                        } else {
                            Platform.runLater(() -> overallStatusLabel.setText("Invalid UPDATE syntax (missing SET or WHERE)."));
                        }
                    } else {
                        Platform.runLater(() -> overallStatusLabel.setText("Invalid UPDATE namespace.set format."));
                    }
                } else {
                    Platform.runLater(() -> overallStatusLabel.setText("Unsupported UPDATE syntax."));
                }} finally {
                    Platform.runLater(() -> progressBar.setVisible(false));
                }
            });
        } else {
            Platform.runLater(() -> {
                overallStatusLabel.setText("Unsupported UPDATE command.");
                progressBar.setVisible(false);
            });
        }
    }

    private Object parseAqlValue(String valueStr) {
        String trimmedValue = valueStr.trim();
        if (trimmedValue.startsWith("'") && trimmedValue.endsWith("'") && trimmedValue.length() >= 2) {
            return trimmedValue.substring(1, trimmedValue.length() - 1);
        } else if (trimmedValue.matches("-?\\d+")) {
            return Integer.parseInt(trimmedValue);
        } else if (trimmedValue.matches("-?\\d+(\\.\\d+)?")) {
            return Double.parseDouble(trimmedValue);
        }
        return trimmedValue; // Treat as string if parsing fails
    }
    private void scanAndDelete(String namespace, String set, String binName, String binValue) {
        executor.submit(() -> {
            Statement stmt = new Statement();
            stmt.setNamespace(namespace);
            stmt.setSetName(set);
            stmt.setFilter(Filter.equal(binName, binValue));

            QueryPolicy queryPolicy = new QueryPolicy();
//            queryPolicy.executeMode = com.aerospike.client.query.ExecuteMode.ALL; // Need to process all records

            List<Key> keysToDelete = new ArrayList<>();

            try (RecordSet recordSet = client.query(queryPolicy, stmt)) {
                recordSet.forEach(record -> {
                    keysToDelete.add(record.key);
                });
            } catch (AerospikeException e) {
                Platform.runLater(() -> overallStatusLabel.setText("Error scanning for delete: " + e.getMessage()));
                e.printStackTrace();
                Platform.runLater(() -> progressBar.setVisible(false));
                return;
            }

            WritePolicy writePolicy = new WritePolicy();
            int deletedCount = 0;
            for (Key keyToDelete : keysToDelete) {
                try {
                    client.delete(writePolicy, keyToDelete);
                    deletedCount++;
                } catch (AerospikeException e) {
                    Platform.runLater(() -> overallStatusLabel.setText("Error deleting key " + keyToDelete + ": " + e.getMessage()));
                    e.printStackTrace();
                }
            }

            int finalDeletedCount = deletedCount;
            Platform.runLater(() -> {
                overallStatusLabel.setText(finalDeletedCount + " records deleted from " + namespace + "." + set + " where " + binName + "='" + binValue + "'");
                progressBar.setVisible(false);
            });
        });
    }
}