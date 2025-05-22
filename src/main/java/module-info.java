module com.example.demo {
    requires javafx.controls;
    requires javafx.fxml;
    requires javafx.web;

    requires org.controlsfx.controls;
    requires com.dlsc.formsfx;
    requires net.synedra.validatorfx;
    requires org.kordamp.ikonli.javafx;
    requires org.kordamp.bootstrapfx.core;
    requires eu.hansolo.tilesfx;
    requires com.almasb.fxgl.all;
    requires java.sql;
    requires aerospike.client;
    requires org.json;
    requires org.kordamp.ikonli.fontawesome5;

    opens com.vikki.aerospike to javafx.fxml;
    exports com.vikki.aerospike;
}