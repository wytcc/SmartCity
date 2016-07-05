package util;

import java.sql.Connection;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoModule {

    private String nameString="10.76.0.182";
	private int port=27017;
	MongoClient mongoClient;
	public DB db;
	public DBCollection coll,coll2,coll3;
    public Connection conn;
    public MongoDatabase mongoDB;
    public MongoCollection mongoDBCollection;
	
	public MongoModule() {
		// TODO Auto-generated constructor stub
		connect();
	}
	
	public void connect() {
		mongoClient=new MongoClient(nameString,port);
		db=mongoClient.getDB("db");
		mongoDB = mongoClient.getDatabase("db");
		coll = db.getCollection("Trajectory2016");
		mongoDBCollection = mongoDB.getCollection("Trajectory2016");
	}
	

}
