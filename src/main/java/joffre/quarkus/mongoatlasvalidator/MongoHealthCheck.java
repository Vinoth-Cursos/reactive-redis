package joffre.quarkus.mongoatlasvalidator;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.bson.Document;

@ApplicationScoped
public class MongoHealthCheck {

    @Inject
    MongoClient mongoClient;

    @PostConstruct
    void test() {
        MongoCollection<Document> collection =
                mongoClient
                        .getDatabase("quarkus-reactive-redis-kafka")
                        .getCollection("healthcheck");

        collection.insertOne(new Document("status", "ok"));

        System.out.println("✔ Mongo Atlas conectado y BD creada correctamente");
    }
}