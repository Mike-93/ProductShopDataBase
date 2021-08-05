import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Field;
import org.bson.BsonDocument;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.Consumer;

import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Aggregates.group;

public class MongoRepository {

    private static MongoCollection<Document> shopCollection;
    private static MongoCollection<Document> productCollection;

    public void MongoRepositoryInitial() {
        MongoClient mongoClient = MongoClients.create("mongodb://127.0.0.1:27017");

        MongoDatabase database = mongoClient.getDatabase("local");

        shopCollection = database.getCollection("Shops");
        productCollection = database.getCollection("products");

        shopCollection.drop();
        productCollection.drop();
    }

    public void addShop(String shopName) {
        ArrayList<String> productList = new ArrayList<>();
        Document shopDocument = new Document()
                .append("name", shopName)
                .append("productList", productList);

        if ((shopCollection.find(BsonDocument.parse("{name: {$eq: \"" + shopName + "\"}}")).first() == null)) {
            shopCollection.insertOne(shopDocument);
            System.out.println("Магазин " + shopName + " добавлен!");
        } else {
            System.out.println("Магазин " + shopName + " уже имеется. Введите другое название магазина");
        }
    }


    public void addProduct(String productName, int price) {
        Document productDocument = new Document()
                .append("name", productName)
                .append("price", price);

        if ((productCollection.find(BsonDocument.parse("{name: {$eq: \"" + productName + "\"}}")).first() == null)) {
            productCollection.insertOne(productDocument);
            System.out.println("Товар " + productName + " добавлен!");
        } else {
            System.out.println("Товар " + productName + " уже имеется. Введите другой товар");
        }
    }

    public void addProductsToShop(String productName, String shopName) {
        BsonDocument queryProduct = BsonDocument.parse("{name: {$eq: \"" + productName + "\"}}");
        BsonDocument queryShop = BsonDocument.parse("{name: {$eq: \"" + shopName + "\"}}");
        try {
            if (!(productCollection.find(queryProduct).first() == null) && !(shopCollection.find(queryShop).first() == null)) {
                if (!((shopCollection.find(queryShop).first().getList("productList", String.class).size() == 0))) {
                    shopCollection.findOneAndUpdate(queryShop, BsonDocument.parse("{$addToSet: {productList: \"" + productName + "\"}}"));
                } else {
                    shopCollection.findOneAndUpdate(queryShop, BsonDocument.parse("{$set: {productList: [\"" + productName + "\"]}}"));
                }
            } else {
                System.out.println("Магазин: " + shopCollection.find(queryShop).first() + ", товар: " + productCollection.find(queryProduct).first());
                System.out.println("Товар или магазин не найден");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void showStatistics() {

        System.out.println("-= Общее количество наименований товаров в каждом магазине =- \n");

        shopCollection.aggregate(Arrays.asList(unwind("$productList"), sortByCount("$name"))).forEach((Consumer<Document>) document -> {
            System.out.println("Наименование магазина: " + document.get("_id") + " - количество товара: " + document.get("count") + "\n");
        });

        System.out.println("-= Средняя цена товара в каждом магазине =- \n");

        shopCollection.aggregate(Arrays.asList(
                lookup("products", "productList", "name", "price"),
                unwind("$price"),
                group("$name", Accumulators.avg("avgPrice", "$price.price"))
        )).forEach((Consumer<Document>) document -> System.out.println("Наименование магазина: " + document.get("_id") + " - Средняя цена товара: " + document.get("avgPrice") + "\n"));

        System.out.println("-= Самый дорогой и самый дешевый товар в каждом магазине =- \n");

        shopCollection.aggregate(Arrays.asList(
                lookup("products", "productList", "name", "price"),
                unwind("$price"),
                sort(new Document("name", 1)
                        .append("price.price", 1)),
                group("$name", Arrays.asList(
                        Accumulators.first("minNameProduct", "$price.name"),
                        Accumulators.last("maxNameProduct", "$price.name"),
                        Accumulators.min("minPriceProduct", "$price.price"),
                        Accumulators.max("maxPriceProduct", "$price.price")
                ))
        )).forEach((Consumer<Document>) document ->
                System.out.println("Наименование магазина: " + document.get("_id") + "\n\t " +
                        "- Cамый дешевый товар: " + document.get("minNameProduct") + " - " + document.get("minPriceProduct") + " руб." + "\n\t " +
                        "- Cамый дорогой товар: " + document.get("maxNameProduct") + " - " + document.get("maxPriceProduct") + " руб." + "\n"));

        System.out.println("-= Количество товаров, дешевле 100 рублей в каждом магазине =- \n");

        shopCollection.aggregate(Arrays.asList(
                lookup("products", "productList", "name", "price"),
                unwind("$price"),
                match(new Document("price.price", new Document("$lt", 100))),
                addFields(new Field<>("count", 1)),
                group("$name", Accumulators.sum("count", "$count"))
        )).forEach((Consumer<Document>) document ->
                System.out.println("Наименование магазина: " + document.get("_id") + "\n\t " +
                        "- Количество товаров, дешевле 100 рублей: " + document.get("count")));
    }
}
