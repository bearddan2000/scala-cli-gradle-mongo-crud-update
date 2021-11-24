package example;

import java.io.UnsupportedEncodingException
import java.net.URLEncoder
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.Filters
import com.mongodb.client.model.Updates
import com.mongodb.client.MongoCollection
import org.bson.Document

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

object App {

  def main(args: Array[String]): Unit = {

    // Mongodb initialization parameters.
    val port_no :Int = 27017;
    val auth_user ="admin";
    val auth_pwd = "admin";
    val host_name = "db";
    val db_name = "test";
    var encoded_pwd = "";

    encoded_pwd = URLEncoder.encode(auth_pwd, "UTF-8");

    // Mongodb connection string.
    val client_url = "mongodb://" + auth_user + ":" + encoded_pwd + "@" + host_name + ":" + port_no + "/" + db_name;
    val uri :MongoClientURI = new MongoClientURI(client_url);

    // Connecting to the mongodb server using the given client uri.
    val mongo_client :MongoClient = new MongoClient(uri);

    // Fetching the database from the mongodb.
    val db :MongoDatabase = mongo_client.getDatabase(db_name);

    val collection :MongoCollection[Document] = db.getCollection("employees");

  //
  // 4.2 Insert new document
  //

    val employee :Document = new Document()
                        .append("first_name", "Joe")
                        .append("last_name", "Smith")
                        .append("title", "Java Developer")
                        .append("years_of_service", 3)
                        .append("skills", Arrays.asList("java", "spring", "mongodb"))
                        .append("manager", new Document()
                                              .append("first_name", "Sally")
                                              .append("last_name", "Johanson"));

    val employee2 :Document = new Document()
                       .append("first_name", "Joe")
                       .append("last_name", "Friday")
                       .append("title", "Business Developer")
                       .append("years_of_service", 3)
                       .append("skills", Arrays.asList("social media", "spreadsheet"))
                       .append("manager", new Document()
                                             .append("first_name", "Sally")
                                             .append("last_name", "Johanson"));

    val list = new ArrayList[Document]();
    list.add(employee);
    list.add(employee2);
    collection.insertMany(list);

    val query = new Document("last_name", "Smith");
    queryResults(collection, query);

    for(it <- db.listCollectionNames()) {
     println("[OUTPUT COLLECTION] " + it);
    }

    allResults(collection);

    collection.updateOne(Filters.eq("last_name", "Friday"), Updates.set("last_name", "Zhou"));
    allResults(collection);

    println("[OUTPUT] Done")
  }

  ///////////////////////////////////////////////////////
  def printResults(results :List[Document]): Unit = {
    for(name <- results) {
     println("[OUTPUT RESULT] " + name);
    }
  }

  ///////////////////////////////////////////////////////
  def queryResults(collection :MongoCollection[Document], query :Document): Unit = {
    val results = new ArrayList[Document]();
    collection.find(query).into(results);
    printResults(results);
  }

  ///////////////////////////////////////////////////////
  def allResults(collection :MongoCollection[Document]): Unit = {
    val results = new ArrayList[Document]();
    collection.find().into(results);
    printResults(results);
  }

}
