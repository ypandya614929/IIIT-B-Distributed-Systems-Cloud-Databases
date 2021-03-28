package upgrad;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import org.bson.Document;

import java.sql.*;
import java.util.Arrays;

import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;

public class CRUDHelper {

    /**
     * Display ALl products
     * @param collection
     */
    public static void displayAllProducts(MongoCollection<Document> collection) {
        System.out.println("------ Displaying All Products ------");
        // collection.find to find rows from mongodb collection
        MongoCursor<Document> cursor = collection.find().cursor();
        try {
            // Iterate through all rows and send row to printSingleCommonAttributes
            // to print on the console
            while (cursor.hasNext()) {
                PrintHelper.printSingleCommonAttributes(cursor.next());
            }
        } finally {
            // close cursor after all rows are displayed
            cursor.close();
        }
        // to print blank line
        System.out.println();
    }

    /**
     * Display top 5 Mobiles
     * @param collection
     */
    public static void displayTop5Mobiles(MongoCollection<Document> collection) {
        System.out.println("------ Displaying Top 5 Mobiles ------");
        // find method of collection instance which takes a Document instance having category
        // as Mobiles which filters data from mongodb whose Category is Mobiles and by using
        // limit only 5 top rows
        MongoCursor<Document> cursor = collection.find(new Document("Category", "Mobiles")).limit(5).cursor();
        try {
            // Iterate through all rows and send row to printAllAttributes
            // to print on the console
            while (cursor.hasNext()) {
                PrintHelper.printAllAttributes(cursor.next());
            }
        } finally {
            // close cursor after all rows are displayed
            cursor.close();
        }
        // to print blank line
        System.out.println();
    }

    /**
     * Display products ordered by their categories in Descending order without auto generated Id
     * @param collection
     */
    public static void displayCategoryOrderedProductsDescending(MongoCollection<Document> collection) {
        System.out.println("------ Displaying Products ordered by categories ------");
        // find method of collection instance after that uses sort which takes a Document
        // instance having category with value -1 which performs descending ordering on
        // category the result will yet be updated by excluding mongoDB default generated ID
        // called using fields in projection
        MongoCursor<Document> cursor = collection.find().sort(new Document("Category", -1)).projection(fields(excludeId())).cursor();
        try {
            // Iterate through all rows and send row to printAllAttributes
            // to print on the console
            while (cursor.hasNext()) {
                PrintHelper.printAllAttributes(cursor.next());
            }
        } finally {
            // close cursor after all rows are displayed
            cursor.close();
        }
        // to print blank line
        System.out.println();
    }


    /**
     * Display number of products in each group
     * @param collection
     */
    public static void displayProductCountByCategory(MongoCollection<Document> collection) {
        System.out.println("------ Displaying Product Count by categories ------");
        // aggregating collection instances with group by Category and the temporary result is
        // stored as an arraylist. Then Accumulators.sum can perform the count operation on the
        // array based on results by the group which initiated with value 1, so at the end result
        // contains total number of rows/data by each Category
        MongoCursor<Document> cursor = collection.aggregate(Arrays.asList(Aggregates.group("$Category", Accumulators.sum("Count", 1)))).cursor();
        try {
            while (cursor.hasNext()) {
                // Iterate through all rows and send row to printProductCountInCategory
                // to print count and category on the console
                PrintHelper.printProductCountInCategory(cursor.next());
            }
        } finally {
            // close cursor after all rows are displayed
            cursor.close();
        }
        // to print blank line
        System.out.println();
    }

    /**
     * Display Wired Headphones
     * @param collection
     */
    public static void displayWiredHeadphones(MongoCollection<Document> collection) {
        System.out.println("------ Displaying Wired headphones ------");
        // find method of collection instance which takes a Document instance having category
        // as Headphones which filters data from mongodb whose Category is Headphones and ConnetoryType as Wired
        MongoCursor<Document> cursor = collection.find(new Document("Category", "Headphones").append("ConnectorType", "Wired")).cursor();
        try {
            // Iterate through all rows and send row to printAllAttributes
            // to print on the console
            while (cursor.hasNext()) {
                PrintHelper.printAllAttributes(cursor.next());
            }
        } finally {
            // close cursor after all rows are displayed
            cursor.close();
        }
        // to print blank line
        System.out.println();
    }
}