package com.vikki.aerospike;

import com.aerospike.client.*;
import com.aerospike.client.Record;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.ScanPolicy;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.beans.property.ReadOnlyObjectWrapper;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.collections.transformation.SortedList;
import javafx.geometry.Pos;
import javafx.geometry.Side;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.control.cell.TextFieldTableCell;
import javafx.scene.image.Image;
import javafx.scene.input.Clipboard;
import javafx.scene.input.ClipboardContent;
import javafx.scene.input.KeyCode;
import javafx.scene.input.KeyCodeCombination;
import javafx.scene.input.KeyCombination;
import javafx.scene.layout.*;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import javafx.scene.web.WebView;
import javafx.util.Callback;
import org.json.JSONArray;
import org.json.JSONObject;
import org.kordamp.ikonli.fontawesome5.FontAwesomeSolid;
import org.kordamp.ikonli.javafx.FontIcon;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Optional;

public class AerospikeExplorer extends Application implements AerospikeDataManipulation.SelectResultCallback {

    private TextField hostInput;
    private Label connectionStatusLabel;
    private TreeView<String> namespaceSetTree;
    private TableView<Map<String, Object>> dataTableView;
    private AerospikeClient client;
    private final ExecutorService scanExecutor = Executors.newSingleThreadExecutor();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private TabPane queryTabPane;
    private Button executeAqlButton;
    private Button helpButton;
    private RadioButton tableOutputRadio;
    private RadioButton jsonOutputRadio;
    private ToggleGroup outputFormatGroup;
    private ScrollPane jsonScrollPane;
    private Label jsonOutputLabel;
    private TextField filterInput;
    private Label statusBarLabel;
    private Button connectButton;
    private Button disconnectButton;
    private Button refreshTreeButton;
    private Button newQueryTabButton;
    private ProgressBar progressBar;

    private TextArea queryTextArea;
    private Button deleteSetButton;
    private static final String APP_STYLE = """
        .root {
            -fx-font-family: "Segoe UI", Tahoma, Geneva, Verdana, sans-serif;
            -fx-font-size: 14px;
            -fx-background-color: #f4f6f8; /* Light background */
        }

        .top-bar {
            -fx-padding: 10px;
            -fx-spacing: 10px;
            -fx-alignment: center-left;
            -fx-background-color: #e9ecef;
            -fx-border-bottom: 1px solid #dee2e6;
        }

        .connection-input {
            -fx-spacing: 5px;
        }

        .connection-input .label {
            -fx-font-weight: bold;
            -fx-text-fill: #495057;
        }

        .connection-input .text-field {
            -fx-padding: 8px;
            -fx-border-color: #ced4da;
            -fx-border-radius: 4px;
            -fx-background-color: white;
        }

        .main-toolbar {
            -fx-padding: 8px;
            -fx-spacing: 8px;
            -fx-alignment: center-left;
            -fx-background-color: #f8f9fa;
            -fx-border-bottom: 1px solid #dee2e6;
        }

        .main-toolbar .button {
            -fx-background-color: #007bff;
            -fx-text-fill: white;
            -fx-padding: 8px 12px;
            -fx-border-radius: 4px;
            -fx-background-radius: 4px;
            -fx-font-weight: bold;
            -fx-border: none;
            -fx-cursor: hand;
        }

        .main-toolbar .button:hover {
            -fx-background-color: #0056b3;
        }

        .left-sidebar {
            -fx-padding: 10px;
            -fx-min-width: 200px;
            -fx-border-right: 1px solid #dee2e6;
            -fx-background-color: #ffffff;
        }

        .left-sidebar .label {
            -fx-font-weight: bold;
            -fx-text-fill: #495057;
            -fx-margin-bottom: 5px;
        }

        .namespace-tree {
            -fx-border-color: #ced4da;
            -fx-border-width: 1px;
            -fx-background-color: white;
            -fx-border-radius: 4px;
        }

        .query-area {
            -fx-padding: 10px;
        }

        .query-area .label {
            -fx-font-weight: bold;
            -fx-text-fill: #495057;
            -fx-margin-bottom: 5px;
        }

        .query-tabs .tab-header-area .tab-header-background {
            -fx-background-color: #f8f9fa;
            -fx-border-bottom: 1px solid #dee2e6;
        }

        .query-tabs .tab {
            -fx-background-color: #e9ecef;
            -fx-focus-color: transparent;
            -fx-border-radius: 4px 4px 0 0;
        }

        .query-tabs .tab:selected {
            -fx-background-color: #ffffff;
            -fx-border-bottom: none;
        }

        .query-tabs .tab .tab-label {
            -fx-text-fill: #495057;
            -fx-font-weight: bold;
        }

        .query-controls-area {
            -fx-padding: 10px 0;
            -fx-spacing: 10px;
            -fx-alignment: center-left;
        }

        .data-area {
            -fx-padding: 10px;
            -fx-min-width: 300px;
            -fx-border-left: 1px solid #dee2e6;
            -fx-background-color: #ffffff;
        }

        .data-area .label {
            -fx-font-weight: bold;
            -fx-text-fill: #495057;
            -fx-margin-bottom: 5px;
        }

        .filter-input {
            -fx-padding: 8px;
            -fx-border-color: #ced4da;
            -fx-border-radius: 4px;
            -fx-background-color: white;
            -fx-margin-bottom: 10px;
        }

        .data-table-view {
            -fx-border-color: #ced4da;
            -fx-border-width: 1px;
            -fx-background-color: white;
            -fx-border-radius: 4px;
        }

        .output-format-bar {
            -fx-padding: 10px;
            -fx-spacing: 10px;
            -fx-alignment: center-right;
            -fx-background-color: #f8f9fa;
            -fx-border-top: 1px solid #dee2e6;
        }

        .output-format-bar .label {
            -fx-font-weight: bold;
            -fx-text-fill: #495057;
        }

        .radio-button .radio {
            -fx-mark-color: #007bff;
        }

        .status-bar {
            -fx-background-color: #e9ecef;
            -fx-padding: 8px 10px;
            -fx-border-top: 1px solid #dee2e6;
            -fx-alignment: center-left;
            -fx-spacing: 10px;
        }

        .status-bar .label {
            -fx-font-weight: normal;
            -fx-text-fill: #6c757d;
        }

        .progress-bar {
            -fx-pref-width: 150px;
        }
    """;

    public static void main(String[] args) {
        launch(args);
    }

    @Override
    public void start(Stage primaryStage) {
        Image appIcon = new Image(Objects.requireNonNull(getClass().getResourceAsStream("/icon.png")));
        primaryStage.getIcons().add(appIcon);
        primaryStage.setTitle("Aerospike Data Explorer");

        // --- Top Bar ---
        HBox topBar = new HBox(15);
        topBar.getStyleClass().add("top-bar");

        GridPane connectionInput = new GridPane();
        connectionInput.getStyleClass().add("connection-input");
        connectionInput.setHgap(5);
        connectionInput.setVgap(5);

        Label hostLabel = new Label("Host:");
        hostInput = new TextField("192.168.29.176:3000,192.168.29.176:3001");
        hostInput.setPrefWidth(300);

        Label userLabel = new Label("User:");
        TextField userInput = new TextField();

        Label passwordLabel = new Label("Password:");
        PasswordField passwordInput = new PasswordField();


        connectionInput.add(hostLabel, 0, 0);       // Column 0, Row 0
        connectionInput.add(userLabel, 1, 0);       // Column 1, Row 0
        connectionInput.add(passwordLabel, 2, 0); // Column 2, Row 0


        connectionInput.add(hostInput, 0, 1);       // Column 0, Row 1
        connectionInput.add(userInput, 1, 1);       // Column 1, Row 1
        connectionInput.add(passwordInput, 2, 1); // Column 2, Row 1




        topBar.getChildren().addAll(connectionInput);
        HBox.setHgrow(connectionInput, Priority.ALWAYS);
        topBar.setAlignment(Pos.CENTER_LEFT);

        // --- Main Toolbar ---
        HBox mainToolbar = new HBox(8);
        mainToolbar.getStyleClass().add("main-toolbar");
        connectButton = new Button("Connect", new FontIcon(FontAwesomeSolid.PLUG));
        disconnectButton = new Button("Disconnect", new FontIcon(FontAwesomeSolid.PLUG));
        disconnectButton.setDisable(true);
        refreshTreeButton = new Button("Refresh", new FontIcon(FontAwesomeSolid.SYNC));
        newQueryTabButton = new Button("New Query", new FontIcon(FontAwesomeSolid.PLUS));
        deleteSetButton = new Button("Delete Set", new FontIcon(FontAwesomeSolid.TRASH));
        mainToolbar.getChildren().addAll(connectButton, disconnectButton, refreshTreeButton, newQueryTabButton, deleteSetButton);

        VBox topSection = new VBox(topBar, mainToolbar);

        connectButton.setOnAction(event -> connectToAerospike(hostInput.getText(), userInput.getText(), passwordInput.getText()));
        disconnectButton.setOnAction(event -> disconnectFromAerospike());
        refreshTreeButton.setOnAction(event -> populateNamespaceSets());
        newQueryTabButton.setOnAction(event -> createNewQueryTab());
        deleteSetButton.setOnAction(event -> deleteSelectedSet());

        // --- Left Sidebar ---
        VBox leftSidebar = new VBox(8);
        leftSidebar.getStyleClass().add("left-sidebar");
        Label namespaceSetLabel = new Label("Namespaces & Sets:");
        namespaceSetTree = new TreeView<>();
        namespaceSetTree.getStyleClass().add("namespace-tree");
        leftSidebar.getChildren().addAll(namespaceSetLabel, namespaceSetTree);
        VBox.setVgrow(namespaceSetTree, Priority.ALWAYS);

        namespaceSetTree.getSelectionModel().selectedItemProperty().addListener((observable, oldValue, newValue) -> {
            if (newValue != null) {
                TreeItem<String> selectedItem = newValue;
                if (selectedItem.getParent() != null && selectedItem.getParent().getValue().equals(namespaceSetTree.getRoot().getValue())) {
                    String namespace = selectedItem.getValue();
                    scanAllAndDisplayInTable(namespace, null);
                } else if (selectedItem.getParent() != null && !selectedItem.getParent().getValue().equals(namespaceSetTree.getRoot().getValue())) {
                    String namespace = selectedItem.getParent().getValue();
                    String set = selectedItem.getValue();
                    scanAllAndDisplayInTable(namespace, set);
                }
            }
        });

        // --- Center - Query Area ---
        VBox queryArea = new VBox(8);
        queryArea.getStyleClass().add("query-area");
        Label queryLabel = new Label("Query:");
        queryTabPane = new TabPane();
        queryTabPane.getStyleClass().add("query-tabs");
        createNewQueryTab(); // Create an initial tab

        HBox queryControlsArea = new HBox(8);
        queryControlsArea.getStyleClass().add("query-controls-area");
        executeAqlButton = new Button("Execute", new FontIcon(FontAwesomeSolid.PLAY));
        helpButton = new Button("Help", new FontIcon(FontAwesomeSolid.QUESTION_CIRCLE));
        queryControlsArea.getChildren().addAll(executeAqlButton, helpButton);
        queryControlsArea.setAlignment(Pos.CENTER_LEFT);

        queryArea.getChildren().addAll(queryLabel, queryTabPane, queryControlsArea);
        VBox.setVgrow(queryTabPane, Priority.ALWAYS);

        executeAqlButton.setOnAction(event -> executeCurrentQuery());
        helpButton.setOnAction(event -> showAqlHelp());

        // --- Right - Data Area ---
        VBox dataArea = new VBox(8);
        dataArea.getStyleClass().add("data-area");
        Label filterLabel = new Label("Filter:");
        filterInput = new TextField();
        filterInput.getStyleClass().add("filter-input");

        dataTableView = new TableView<>();
        dataTableView.getStyleClass().add("data-table-view");
        VBox.setVgrow(dataTableView, Priority.ALWAYS);
        // Add Serial Number Column
        TableColumn<Map<String, Object>, Integer> serialNumberColumn = new TableColumn<>("Serial No.");
        serialNumberColumn.setMinWidth(50);
        serialNumberColumn.setCellValueFactory(param -> new ReadOnlyObjectWrapper<>(dataTableView.getItems().indexOf(param.getValue()) + 1));

        // Add other columns as needed
        dataTableView.getColumns().add(serialNumberColumn);
        jsonScrollPane = new ScrollPane();
        jsonScrollPane.setFitToWidth(true);
        jsonScrollPane.setPrefHeight(200);


        dataArea.getChildren().addAll(filterLabel, filterInput, dataTableView);

        // --- Bottom Bar - Output Format and Status ---

        HBox statusBar = new HBox(8);
        statusBar.getStyleClass().add("status-bar");
        statusBarLabel = new Label("Not Connected");
        connectionStatusLabel = statusBarLabel;
        progressBar = new ProgressBar();
        progressBar.setVisible(false);
        statusBar.getChildren().addAll(statusBarLabel, progressBar);
        HBox.setHgrow(statusBarLabel, Priority.ALWAYS);






// --- Export Buttons ---
//        HBox exportButtons = new HBox(8);
//        exportButtons.getStyleClass().add("export-buttons");
//        Button exportJsonButton = new Button("Export JSON", new FontIcon(FontAwesomeSolid.FILE));
//        Button exportCsvButton = new Button("Export CSV", new FontIcon(FontAwesomeSolid.FILE));
//        exportButtons.getChildren().addAll(exportJsonButton, exportCsvButton);
//        exportButtons.setAlignment(Pos.CENTER_RIGHT);

        HBox exportButtons = new HBox(8);
        exportButtons.getStyleClass().add("export-buttons");

        Button exportButton = new Button("Export", new FontIcon(FontAwesomeSolid.FILE_EXPORT));
        ComboBox<String> exportFormat = new ComboBox<>();
        exportFormat.getItems().addAll("JSON", "CSV");
        exportFormat.setValue("JSON");

        exportButtons.getChildren().addAll(exportButton, exportFormat);
        exportButtons.setAlignment(Pos.CENTER_RIGHT);



// --- Main Layout with Export Buttons ---
        BorderPane rootLayout = new BorderPane();
        SplitPane horizontalSplitPane = new SplitPane(leftSidebar, new VBox(queryArea, new Separator(), dataArea));
        horizontalSplitPane.setDividerPositions(0.2, 0.6);
        rootLayout.setTop(topSection);
        rootLayout.setCenter(horizontalSplitPane);
        rootLayout.setBottom(new VBox(new Separator(), statusBar, exportButtons)); // Separator for visual distinction



// --- Export JSON ---
        exportButton.setOnAction(event -> {
            String format = exportFormat.getValue();
            List<Map<String, Object>> data = dataTableView.getItems();
            if (data.isEmpty()) {
                statusBarLabel.setText("No data to export.");
                return;
            }

            // Create a context menu for save and copy options
            ContextMenu contextMenu = new ContextMenu();
            MenuItem saveItem = new MenuItem("Save as");
            MenuItem copyItem = new MenuItem("Copy as");

            contextMenu.getItems().addAll(saveItem, copyItem);

            if ("JSON".equals(format)) {
                String jsonContent = convertToJson(data);
                saveItem.setOnAction(e -> saveToFile(jsonContent, "json",primaryStage));
                copyItem.setOnAction(e -> copyToClipboard(jsonContent));
            } else if ("CSV".equals(format)) {
                String csvContent = convertToCsv(data);
                saveItem.setOnAction(e -> saveToFile(csvContent, "csv",primaryStage));
                copyItem.setOnAction(e -> copyToClipboard(csvContent));
            }

            contextMenu.show(exportButton, Side.BOTTOM, 0, 0);
        });

// --- Export JSON ---




        Scene scene = new Scene(rootLayout, 1600, 900);
        scene.getStylesheets().add("data:, " + APP_STYLE);

        // --- Keyboard Shortcut for Execute ---
        scene.getAccelerators().put(
                new KeyCodeCombination(KeyCode.ENTER, KeyCombination.CONTROL_DOWN),
                this::executeCurrentQuery
        );

        primaryStage.setScene(scene);
        primaryStage.show();
    }
    String convertToJson(List<Map<String, Object>> data) {
        JSONArray jsonArray = new JSONArray();
        for (Map<String, Object> record : data) {
            jsonArray.put(new JSONObject(record));
        }
        return jsonArray.toString(2);
    }

    // --- Convert to CSV ---
    String convertToCsv(List<Map<String, Object>> data) {
        StringBuilder csvContent = new StringBuilder();
        if (!data.isEmpty()) {
            Set<String> columnNames = new HashSet<>();
            for (Map<String, Object> record : data) {
                columnNames.addAll(record.keySet());
            }
            csvContent.append(String.join(",", columnNames)).append("\n");
            for (Map<String, Object> record : data) {
                List<String> row = new ArrayList<>();
                for (String columnName : columnNames) {
                    row.add(record.getOrDefault(columnName, "").toString());
                }
                csvContent.append(String.join(",", row)).append("\n");
            }
        }
        return csvContent.toString();
    }

    // --- Save to File ---
    void saveToFile(String content, String extension, Stage primaryStage) {
        // Generate a timestamped filename
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"));
        String defaultFileName = "exported_data_" + timestamp + "." + extension;

        FileChooser fileChooser = new FileChooser();
        fileChooser.setTitle("Save File");
        fileChooser.setInitialFileName(defaultFileName);
        String userHome = System.getProperty("user.home");
        File downloadsFolder = new File(userHome, "Downloads");
        fileChooser.setInitialDirectory(downloadsFolder);

        fileChooser.getExtensionFilters().add(new FileChooser.ExtensionFilter("Files", "*." + extension));
        File file = fileChooser.showSaveDialog(primaryStage);

        if (file != null) {
            try (FileWriter writer = new FileWriter(file)) {
                writer.write(content);
                writer.flush();
                statusBarLabel.setText("Data exported as ." + extension + " to " + file.getAbsolutePath());
                showInfoDialog("Export Successful", "Data exported as ." + extension + " to " + file.getAbsolutePath(), Alert.AlertType.INFORMATION);
            } catch (IOException e) {
                statusBarLabel.setText("Error exporting: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    // --- Copy to Clipboard ---
    void copyToClipboard(String content) {
        ClipboardContent clipboardContent = new ClipboardContent();
        clipboardContent.putString(content);
        Clipboard.getSystemClipboard().setContent(clipboardContent);
        statusBarLabel.setText("Data copied to clipboard");
        showInfoDialog("Export Successful","Data copied to clipboard", Alert.AlertType.INFORMATION);

    }

    void showInfoDialog(String title, String message, Alert.AlertType alertType) {
        Alert alert = new Alert(alertType);
        alert.setTitle(title);
        alert.setHeaderText(null);
        alert.setContentText(message);
        alert.showAndWait();
    }

    private void createNewQueryTab() {
        TextArea queryTextArea = new TextArea();
        Tab newTab = new Tab("New Query", queryTextArea);
        newTab.setClosable(true);

        newTab.setOnSelectionChanged(event -> {
            if (newTab.isSelected() && newTab.getText().equals("New Query")) {
                // Optionally, you could
                // prompt for a name when the tab is first selected
            }
        });

        ContextMenu tabContextMenu = new ContextMenu();
        MenuItem renameTabMenuItem = new MenuItem("Rename");
        renameTabMenuItem.setOnAction(event -> {
            TextInputDialog dialog = new TextInputDialog(newTabgetText());
            dialog.setTitle("Rename Tab");
            dialog.setHeaderText("Enter new name for the tab:");
            dialog.setContentText("New Name:");

            Optional<String> result = dialog.showAndWait();
            result.ifPresent(name -> newTab.setText(name.trim().isEmpty() ? "New Query" : name));
        });
        tabContextMenu.getItems().add(renameTabMenuItem);
        newTab.setContextMenu(tabContextMenu);

        newTab.setOnClosed(event -> {
            if (queryTabPane.getTabs().isEmpty()) {
                createNewQueryTab(); // Ensure at least one tab exists
            }
        });

        queryTabPane.getTabs().add(newTab);
        queryTabPane.getSelectionModel().select(newTab);
    }

    private String getCurrentQuery() {
        Tab selectedTab = queryTabPane.getSelectionModel().getSelectedItem();
        if (selectedTab != null && selectedTab.getContent() instanceof TextArea) {
            return ((TextArea) selectedTab.getContent()).getText().trim();
        }
        return null;
    }

    private void executeCurrentQuery() {
        String aql = getCurrentQuery();
        if (aql != null && !aql.isEmpty()) {
            String command = aql.split("\\s+")[0].toUpperCase();
            System.out.println("Execute AQL Button Pressed. Command: " + command + ", AQL: " + aql);
            if (client != null && client.isConnected()) {
                progressBar.setVisible(true);
                statusBarLabel.setText("Executing query...");
                if (command.equals("INSERT") || command.equals("DELETE") || command.equals("UPDATE") || command.equals("SELECT")) {
                    AerospikeDataManipulation dataManipulation = new AerospikeDataManipulation(client, scanExecutor, statusBarLabel, dataTableView, progressBar, statusBarLabel, tableOutputRadio, jsonOutputRadio); // Pass progress bar and status label
                    dataManipulation.setSelectResultCallback((AerospikeDataManipulation.SelectResultCallback) this);
                    dataManipulation.executeAqlManipulation(aql);
                } else {
                    Platform.runLater(() -> {
                        statusBarLabel.setText("Unsupported AQL command: " + command);
                        progressBar.setVisible(false);
                    });
                }
            } else {
                Platform.runLater(() -> {
                    statusBarLabel.setText("Not connected to Aerospike.");
                    progressBar.setVisible(false);
                });
            }
        } else {
            Platform.runLater(() -> {
                statusBarLabel.setText("No query to execute.");
                progressBar.setVisible(false);
            });
        }
    }

    private void connectToAerospike(String hosts, String user, String password) {
        try {
            progressBar.setVisible(true);
            statusBarLabel.setText("Connecting...");
            ClientPolicy clientPolicy = new ClientPolicy();
            if (!user.isEmpty() && !password.isEmpty()) {
                clientPolicy.user = user;
                clientPolicy.password = password;
            }
            String[] hostArray = hosts.split(",");
            List<Host> hostList = new ArrayList<>();
            for (String host : hostArray) {
                String[] hostPort = host.split(":");
                if (hostPort.length == 2) {
                    hostList.add(new Host(hostPort[0].trim(), Integer.parseInt(hostPort[1].trim())));
                } else {
                    hostList.add(new Host(host.trim(), 3000)); // Default port 3000
                }
            }
            client = new AerospikeClient(clientPolicy, hostList.toArray(new Host[0]));
            if (client.isConnected()) {
                statusBarLabel.setText("Connected to " + hosts);
                disconnectButton.setDisable(false);
                connectButton.setDisable(true);
                populateNamespaceSets();
            } else {
                statusBarLabel.setText("Connection Failed");
            }
        } catch (AerospikeException e) {
            statusBarLabel.setText("Connection Error: " + e.getMessage());
            client = null;
            disconnectButton.setDisable(true);
            connectButton.setDisable(false);
            e.printStackTrace();
        } finally {
            progressBar.setVisible(false);
        }
    }

    private void disconnectFromAerospike() {
        if (client != null && client.isConnected()) {
            progressBar.setVisible(true);
            statusBarLabel.setText("Disconnecting...");
            executor.submit(() -> {
                client.close();
                client = null;
                Platform.runLater(() -> {
                    statusBarLabel.setText("Disconnected");
                    disconnectButton.setDisable(true);
                    connectButton.setDisable(false);
                    namespaceSetTree.setRoot(null);
                    dataTableView.getItems().clear();
                    dataTableView.getColumns().clear();
                    progressBar.setVisible(false);
                });
            });
        } else {
            statusBarLabel.setText("Not connected");
        }
    }

    private void populateNamespaceSets() {
        if (client != null && client.isConnected()) {
            progressBar.setVisible(true);
            statusBarLabel.setText("Fetching namespaces and sets...");
            TreeItem<String> rootItem = new TreeItem<>("Aerospike");
            rootItem.setExpanded(true);
            namespaceSetTree.setRoot(rootItem);

            executor.submit(() -> {
                try {
                    Map<String, Set<String>> namespaceSetsMap = new HashMap<>();
                    Node[] nodes = client.getNodes();

                    for (Node node : nodes) {
                        String namespacesInfo = Info.request(null, node, "namespaces");
                        if (namespacesInfo != null && !namespacesInfo.isEmpty()) {
                            String[] namespaces = namespacesInfo.split(";");
                            for (String namespace : namespaces) {
                                if (!namespaceSetsMap.containsKey(namespace)) {
                                    namespaceSetsMap.put(namespace, new HashSet<>());
                                }
                                String setsInfo = Info.request(null, node, "sets/" + namespace);
                                if (setsInfo != null && !setsInfo.isEmpty()) {
                                    String[] sets = setsInfo.split(";");
                                    for (String setInfo : sets) {
                                        String setName = setInfo.split(":")[1].split("=")[1];
                                        namespaceSetsMap.get(namespace).add(setName);
                                    }
                                }
                            }
                        }
                    }

                    Platform.runLater(() -> {
                        namespaceSetsMap.forEach((namespace, sets) -> {
                            TreeItem<String> namespaceItem = new TreeItem<>(namespace);
                            sets.stream().sorted().forEach(set -> namespaceItem.getChildren().add(new TreeItem<>(set)));
                            rootItem.getChildren().add(namespaceItem);
                        });
                        statusBarLabel.setText("Namespaces and sets loaded.");
                        progressBar.setVisible(false);
                    });

                } catch (AerospikeException e) {
                    Platform.runLater(() -> {
                        statusBarLabel.setText("Error retrieving namespaces and sets: " + e.getMessage());
                        progressBar.setVisible(false);
                    });
                    e.printStackTrace();
                }
            });
        } else {
            statusBarLabel.setText("Not Connected");
        }
    }

    private void scanAllAndDisplayInTable(String namespace, String set) {
        System.out.println("scanAllAndDisplayInTable called with namespace: " + namespace + ", set: " + set);
        if (client != null && client.isConnected()) {
            ObservableList<Map<String, Object>> allRecords = FXCollections.observableArrayList();
            Platform.runLater(() -> {
                dataTableView.getItems().clear();
                dataTableView.getColumns().clear();
                dataTableView.setPlaceholder(new Label("Scanning data..."));
                progressBar.setVisible(true);
                statusBarLabel.setText("Scanning " + namespace + (set != null ? "." + set : "") + "...");
            });

            scanExecutor.submit(() -> {
                try {
                    System.out.println("Starting scan for namespace: " + namespace + ", set: " + set);
                    client.scanAll(new ScanPolicy(), namespace, set, (key, record) -> {
                        System.out.println("Record found in scan: Key=" + key);
                        Map<String, Object> recordData = new HashMap<>();
                        recordData.put("Namespace", key.namespace);
                        recordData.put("Set", key.setName);
                        if (record != null) {
                            recordData.put("Generation", record.generation);
                            recordData.put("TTL", record.getTimeToLive());
                            if (record.bins != null) {
                                recordData.putAll(record.bins);
                            }
                        }
                        Platform.runLater(() -> allRecords.add(recordData));
                    });
                    System.out.println("Scan completed for namespace: " + namespace + ", set: " + set);

                    Platform.runLater(() -> {
                        updateTableView(allRecords);
                        statusBarLabel.setText("Scan completed.");
                        progressBar.setVisible(false);
                    });

                } catch (AerospikeException e) {
                    Platform.runLater(() -> {
                        dataTableView.setPlaceholder(new Label("Error during scan: " + e.getMessage()));
                        statusBarLabel.setText("Error during scan: " + e.getMessage());
                        progressBar.setVisible(false);
                    });
                    e.printStackTrace();
                }
            });
        } else {
            dataTableView.setPlaceholder(new Label("Not Connected to Aerospike."));
            statusBarLabel.setText("Not Connected to Aerospike.");
        }
    }

    @Override
    public void onResult(java.util.List<Map<String, Object>> results) {
        Platform.runLater(() -> {
            progressBar.setVisible(false);
            statusBarLabel.setText("Query executed.");
            if (results != null) {
                updateTableView(FXCollections.observableArrayList(results));
                if (outputFormatGroup.getSelectedToggle() == jsonOutputRadio) {
                    displayJsonOutput(new ArrayList<>(results));
                }
            } else {
                dataTableView.getItems().clear();
                dataTableView.getColumns().clear();
                dataTableView.setPlaceholder(new Label("No results from query."));
            }
        });
    }

    @Override
    public void onError(String errorMessage) {
        Platform.runLater(() -> {
            progressBar.setVisible(false);
            statusBarLabel.setText("Query Error: " + errorMessage);
            dataTableView.setPlaceholder(new Label("Query Error: " + errorMessage));
            jsonOutputLabel.setText("Query Error: " + errorMessage);
            jsonScrollPane.setContent(new Label(errorMessage));
            jsonScrollPane.setVisible(true);
            tableOutputRadio.setSelected(false);
            jsonOutputRadio.setSelected(true);
            System.err.println("Query Error: " + errorMessage);
        });
    }

    private void updateTableView(ObservableList<Map<String, Object>> results) {
        if (!results.isEmpty()) {
            dataTableView.getColumns().clear();

            Set<String> columnNames = new HashSet<>();
            for (Map<String, Object> result : results) {
                columnNames.addAll(result.keySet());
            }

            TableColumn<Map<String, Object>, Integer> serialNumberColumn = new TableColumn<>("Serial No.");
            serialNumberColumn.setMinWidth(50);
            serialNumberColumn.setCellValueFactory(param -> new ReadOnlyObjectWrapper<>(dataTableView.getItems().indexOf(param.getValue()) + 1));


            dataTableView.getColumns().add(serialNumberColumn);
            List<TableColumn<Map<String, Object>, String>> columns = new ArrayList<>();
            for (String columnName : columnNames) {
                TableColumn<Map<String, Object>, String> column = new TableColumn<>(columnName);
                column.setCellValueFactory(cellData -> {
                    Object value = cellData.getValue().get(columnName);
                    return new SimpleStringProperty(value != null ? value.toString() : "");
                });
                if (!columnName.equals("Key") && !columnName.equals("Namespace") && !columnName.equals("Set")) {
                    column.setCellFactory(TextFieldTableCell.forTableColumn());
                    column.setOnEditCommit(event -> {
                        Map<String, Object> record = event.getRowValue();
                        record.put(columnName, event.getNewValue());
                        String namespace = (String) record.get("Namespace");
                        String set = (String) record.get("Set");
                        String key = (String) record.get("Key");
                        String binName = event.getTableColumn().getText();
                        String newValue = event.getNewValue();
                        executeUpdateRecord(namespace, set, key, binName, newValue);
                        System.out.println("Updated record: " + record);
                    });
                }
                columns.add(column);
            }
            dataTableView.getColumns().addAll(columns);

            // Add Delete Button Column
            TableColumn<Map<String, Object>, Void> deleteCol = new TableColumn<>("Action");
            deleteCol.setCellFactory(new Callback<TableColumn<Map<String, Object>, Void>, TableCell<Map<String, Object>, Void>>() {
                @Override
                public TableCell<Map<String, Object>, Void> call(final TableColumn<Map<String, Object>, Void> param) {
                    final TableCell<Map<String, Object>, Void> cell = new TableCell<Map<String, Object>, Void>() {
                        final Button deleteButton = new Button("", new FontIcon(FontAwesomeSolid.TRASH));
                        {
                            deleteButton.setOnAction(event -> {
                                Map<String, Object> data = getTableView().getItems().get(getIndex());
                                String namespace = (String) data.get("Namespace");
                                String set = (String) data.get("Set");
                                String key = (String) data.get("Key");
                                executeDeleteRecord(namespace, set, key);
                            });
                        }

                        @Override
                        protected void updateItem(Void item, boolean empty) {
                            super.updateItem(item, empty);
                            if (empty) {
                                setGraphic(null);
                            } else {
                                setGraphic(deleteButton);
                            }
                        }
                    };
                    return cell;
                }
            });
            dataTableView.getColumns().add(deleteCol);

            // Context Menu for Table
            ContextMenu tableContextMenu = new ContextMenu();
            MenuItem copyValueMenuItem = new MenuItem("Copy Value");
            copyValueMenuItem.setOnAction(event -> {
                TablePosition<?, ?> pos = dataTableView.getSelectionModel().getSelectedCells().get(0);
                if (pos != null) {
                    int row = pos.getRow();
                    TableColumn<?, ?> col = pos.getTableColumn();
                    Object value = col.getCellData(row);
                    if (value != null) {
                        ClipboardContent content = new ClipboardContent();
                        content.putString(value.toString());
                        Clipboard.getSystemClipboard().setContent(content);
                    }
                }
            });
            tableContextMenu.getItems().add(copyValueMenuItem);
            dataTableView.setContextMenu(tableContextMenu);

            FilteredList<Map<String, Object>> filteredData = new FilteredList<>(results, p -> true);
            filterInput.textProperty().addListener((observable, oldValue, newValue) -> {
                filteredData.setPredicate(record -> {
                    if (newValue == null || newValue.isEmpty()) {
                        return true;
                    }
                    String lowerCaseFilter = newValue.toLowerCase();
                    return record.values().stream()
                            .anyMatch(value -> value != null && value.toString().toLowerCase().contains(lowerCaseFilter));
                });
            });

            SortedList<Map<String, Object>> sortedData = new SortedList<>(filteredData);
            sortedData.comparatorProperty().bind(dataTableView.comparatorProperty());
            dataTableView.setItems(sortedData);
            dataTableView.setEditable(true);


        } else {
            dataTableView.setPlaceholder(new Label("No data found."));
        }
    }

    private void executeDeleteRecord(String namespace, String set, String key) {
        if (client != null && client.isConnected() && namespace != null && set != null && key != null) {
            progressBar.setVisible(true);
            statusBarLabel.setText("Deleting record...");
            executor.submit(() -> {
                try {
                    Key aerospikeKey = new Key(namespace, set, key);
                    client.delete(null, aerospikeKey);
                    Platform.runLater(() -> {
                        statusBarLabel.setText("Record with key '" + key + "' deleted.");
                        scanAllAndDisplayInTable(namespace, set);
                        progressBar.setVisible(false);
                    });
                } catch (AerospikeException e) {
                    Platform.runLater(() -> {
                        statusBarLabel.setText("Error during deletion: " + e.getMessage());
                        progressBar.setVisible(false);
                    });
                    e.printStackTrace();
                }
            });
        } else {
            Platform.runLater(() -> statusBarLabel.setText("Not connected or invalid delete parameters."));
        }
    }

    private void executeUpdateRecord(String namespace, String set, String key, String binName, String newValue) {
        if (client != null && client.isConnected() && namespace != null && set != null && key != null && binName != null) {
            progressBar.setVisible(true);
            statusBarLabel.setText("Updating record...");
            executor.submit(() -> {
                try {
                    Key aerospikeKey = new Key(namespace, set, key);
                    Record existingRecord = client.get(null, aerospikeKey, binName);
                    Object parsedValue = newValue;
                    String valueType = "String";

                    if (existingRecord != null && existingRecord.bins != null && existingRecord.bins.containsKey(binName)) {
                        Object existingValue = existingRecord.bins.get(binName);
                        if (existingValue instanceof Integer) {
                            valueType = "Integer";
                            try {
                                parsedValue = Integer.parseInt(newValue);
                            } catch (NumberFormatException e) {
                                Platform.runLater(() -> statusBarLabel.setText("Error: Cannot update " + binName + " (Integer) with '" + newValue + "'. Treating as String."));
                                parsedValue = newValue; // Treat as string on parse failure
                            }
                        } else if (existingValue instanceof Double) {
                            valueType = "Double";
                            try {
                                parsedValue= Double.parseDouble(newValue);
                            } catch (NumberFormatException e) {
                                Platform.runLater(() -> statusBarLabel.setText("Error: Cannot update " + binName + " (Double) with '" + newValue + "'. Treating as String."));
                                parsedValue = newValue; // Treat as string on parse failure
                            }
                        } else if (existingValue instanceof Boolean) {
                            valueType = "Boolean";
                            parsedValue = Boolean.parseBoolean(newValue);
                        }
                    }

                    Bin binToUpdate = new Bin(binName, parsedValue);
                    client.put(null, aerospikeKey, binToUpdate);
                    String finalValueType = valueType;
                    Platform.runLater(() -> {
                        statusBarLabel.setText("Record updated: " + namespace + "." + set + "." + key + " (" + binName + " as " + finalValueType + ")");
                        scanAllAndDisplayInTable(namespace, set); // Refresh the table after update
                        progressBar.setVisible(false);
                    });
                } catch (AerospikeException e) {
                    Platform.runLater(() -> {
                        statusBarLabel.setText("Error during update: " + e.getMessage());
                        progressBar.setVisible(false);
                    });
                    e.printStackTrace(); // Log the error for debugging
                }
            });
        } else {
            Platform.runLater(() -> statusBarLabel.setText("Not connected or invalid update parameters."));
        }
    }

    private void showAqlHelp() {
        WebView webView = new WebView();
        String helpContent = """
            <!DOCTYPE html>
            <html>
            <head>
            <title>AQL Help</title>
            <style>
              body {
                font-family: Arial, sans-serif;
                margin: 20px;
                padding: 20px;
                background-color: #f4f4f4;
                color: #333;
                line-height: 1.6;
              }
              h1 {
                color: #007bff;
                text-align: center;
                border-bottom: 2px solid #007bff;
                padding-bottom: 10px;
                margin-bottom: 20px;
              }
              h2 {
                color: #28a745;
                margin-top: 30px;
                border-bottom: 1px solid #28a745;
                padding-bottom: 5px;
              }
              p {
                margin-bottom: 10px;
              }
              ul {
                list-style-type: disc;
                padding-left: 20px;
                margin-bottom: 20px;
              }
              li {
                margin-bottom: 5px;
              }
              pre {
                background-color: #e9ecef;
                padding: 10px;
                border: 1px solid #ced4da;
                border-radius: 5px;
                overflow-x: auto;
                margin: 10px 0;
                font-family: monospace;
                font-size: 14px;
                line-height: 1.4;
              }
              .important {
                color: #dc3545;
                font-weight: bold;
              }
            </style>
            </head>
            <body>
            <h1>AQL Help</h1>
            <p>This section provides basic help for using AQL (Aerospike Query Language) in this tool. Currently, only a very limited subset of AQL is supported.</p>
            <h2>Supported AQL Query</h2>
            <p>The current version of this tool supports the following basic SELECT query:</p>
            <pre><code>SELECT * FROM namespace.set [WHERE binName = 'value']</code></pre>
            <p>This query retrieves all records from the specified namespace and set, and can optionally filter by a single condition.</p>
            <h2>Examples</h2>
            <ul>
              <li><strong>Retrieve all records from the 'users' set in the 'test' namespace:</strong></li>
              <pre><code>SELECT * FROM test.users</code></pre>
               <li><strong>Retrieve records from the 'users' set in the 'test' namespace where the bin 'age' is 30:</strong></li>
               <pre><code>SELECT * FROM test.users WHERE age = '30'</code></pre>
               <li><strong>Retrieve records from the 'products' set in the 'mydb' namespace where the bin 'category' is 'electronics':</strong></li>
               <pre><code>SELECT * FROM mydb.products WHERE category = 'electronics'</code></pre>
            </ul>
            <h2>Important Notes</h2>
            <ul style="list-style-type: disc;">
              <li><span class="important">Replace 'namespace' and 'set'</span> with your actual Aerospike namespace and set names.</li>
              <li>The <code>WHERE</code> clause currently only supports equality on a single bin.</li>
              <li>String values in the <code>WHERE</code> clause must be enclosed in single quotes.</li>
              <li>This tool <span class="important">does NOT</span> currently support more complex AQL queries, such as:
                <ul style="list-style-type: circle;">
                  <li>Selecting specific bins (columns)</li>
                  <li>Filtering records with complex WHERE clauses (>, <, AND, OR, etc.)</li>
                  <li>Ordering results</li>
                  <li>Aggregations</li>
                </ul>
              </li>
              <li>The AQL keywords (<code>SELECT</code>, <code>FROM</code>, <code>WHERE</code>) are case-insensitive, but namespace, set, and bin names are case-sensitive.</li>
              <li>Ensure that the namespace and set you specify exist in your Aerospike database and contain data.</li>
            </ul>
            <h2>Limitations</h2>
            <p>This is a very basic implementation of AQL support. The following limitations apply:</p>
            <ul>
              <li>Only the <code>SELECT * FROM namespace.set [WHERE bin = 'value']</code> syntax is supported.</li>
              <li>No support for complex <code>WHERE</code> clauses, bin selection, or other advanced AQL features.</li>
              <li>Error handling is basic.</li>
            </ul>
            <h2>Future Enhancements</h2>
            <p>Future versions of this tool may include:</p>
            <ul>
              <li>Support for more complete AQL syntax.</li>
              <li>A query builder UI to help construct AQL queries.</li>
              <li>Improved error handling and feedback.</li>
              <li>Display of query execution statistics.</li>
            </ul>
            </body>
            </html>
            """;
        webView.getEngine().loadContent(helpContent);

        Scene scene = new Scene(webView, 800, 600);
        Stage popupStage = new Stage();
        popupStage.setTitle("AQL Help");
        popupStage.setScene(scene);
        popupStage.show();
    }

    private void displayJsonOutput(List<Map<String, Object>> results) {
        JSONArray jsonArray = new JSONArray();
        for (Map<String, Object> record : results) {
            JSONObject jsonRecord = new JSONObject(record);
            jsonArray.put(jsonRecord);
        }
        jsonScrollPane.setContent(new Label(jsonArray.toString(2)));
    }
    private void deleteSelectedSet() {
        TreeItem<String> selectedItem = namespaceSetTree.getSelectionModel().getSelectedItem();
        if (selectedItem != null && selectedItem.getParent() != null && !selectedItem.getParent().getValue().equals(namespaceSetTree.getRoot().getValue())) {
            String namespace = selectedItem.getParent().getValue();
            String set = selectedItem.getValue();

            Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
            alert.setTitle("Confirm Delete");
            alert.setHeaderText("Are you sure you want to delete the set?");
            alert.setContentText("Namespace: " + namespace + ", Set: " + set);

            Optional<ButtonType> result = alert.showAndWait();
            if (result.isPresent() && result.get() == ButtonType.OK) {
                executor.submit(() -> {
                    try {
                        client.truncate(new InfoPolicy(), namespace, set, null);
//                        client.truncate(null, namespace, set, null);
                        Platform.runLater(() -> {
                            populateNamespaceSets(); // Refresh the tree view
                            statusBarLabel.setText("Set deleted: " + namespace + "." + set);
                        });
                    } catch (AerospikeException e) {
                        Platform.runLater(() -> statusBarLabel.setText("Error deleting set: " + e.getMessage()));
                    }
                });
            }
        } else {
            statusBarLabel.setText("No set selected for deletion.");
        }
    }

    //    @Override
//    public void onResult(List<Map<String, Object>> results) {
//        Platform.runLater(() -> {
//            // Update the TableView with the results
//            updateTableView(FXCollections.observableArrayList(results));
//        });
//    }
    @Override
    public void stop() {
        if (client != null) {
            client.close();
        }
        scanExecutor.shutdown();
    }
    private String newTabgetText() {
        Tab selectedTab = queryTabPane.getSelectionModel().getSelectedItem();
        if (selectedTab != null) {
            return selectedTab.getText();
        }
        return "New Query"; // Default text if no tab is selected
    }
}