import org.bson.json.JsonParseException;

import java.util.*;
import java.io.IOException;

public class Main {

    private static String[] textSplit;

    public static void main(String[] args) throws IOException, JsonParseException {

        MongoRepository mongoRepository = new MongoRepository();

        mongoRepository.MongoRepositoryInitial();

        printInstructions();

        Scanner scanner = new Scanner(System.in);

        try {
            while (true) {
                System.out.println("Введите команду:");
                String text = scanner.nextLine();

                textSplit = text.split("\\s");
                String command = textSplit[0];
                if (command.equals("EXIT")) {
                    break;
                }
                switch (command) {
                    case "ДОБАВИТЬ_МАГАЗИН":
                        String shopName = textSplit[1];
                        mongoRepository.addShop(shopName);
                        break;
                    case "ДОБАВИТЬ_ТОВАР":
                        String productName = textSplit[1];
                        int price = Integer.parseInt(textSplit[2]);
                        mongoRepository.addProduct(productName, price);
                        break;
                    case "ВЫСТАВИТЬ_ТОВАР":
                        productName = textSplit[1];
                        shopName = textSplit[2];
                        mongoRepository.addProductsToShop(productName, shopName);
                        break;
                    case "СТАТИСТИКА_ТОВАРОВ":
                        mongoRepository.showStatistics();
                        break;
                    case "?":
                        printInstructions();
                        break;
                    default:
                        System.out.println("Команда не распознана");
                        printInstructions();
                        break;
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    public static void printInstructions() {

        System.out.println("Примеры ввода команд:"
                + "\n - ДОБАВИТЬ_МАГАЗИН Девяточка"
                + "\n - ДОБАВИТЬ_ТОВАР Вафли 54"
                + "\n - ВЫСТАВИТЬ_ТОВАР Вафли Девяточка"
                + "\n - СТАТИСТИКА_ТОВАРОВ"
                + "\n - EXIT - завершает работу программы");
    }
}