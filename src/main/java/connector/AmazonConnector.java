package connector;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
<<<<<<< HEAD
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.util.Tables;
import com.amazonaws.services.rds.model.SourceType;
import com.sun.xml.internal.bind.v2.schemagen.xmlschema.AttributeType;

import java.util.ArrayList;
import java.util.List;
=======
import com.amazonaws.services.dynamodbv2.util.Tables;

import java.util.HashMap;
import java.util.Map;
>>>>>>> c87e89087e2422f840744d9782cdd931ddbc1822


/**
 * Created by remy on 09/10/15.
 */
public class AmazonConnector {
    public static String usernane = "Remy";
    public static int compt = 3;

    public static void main(String [] args){
        ProfileCredentialsProvider provider = new ProfileCredentialsProvider(usernane);

        AWSCredentials credentials = provider.getCredentials();
        AmazonDynamoDBClient client = new AmazonDynamoDBClient(credentials);
        client.setRegion(Region.getRegion(Regions.EU_WEST_1));

        DynamoDB dynamoDB = new DynamoDB(client);

<<<<<<< HEAD
        if(Tables.doesTableExist(client, "DoubleLibrary")){
            Table dl = dynamoDB.getTable("DoubleLibrary");
            client.deleteTable("DoubleLibrary");
            try {
                dl.waitForDelete();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

=======
        if (Tables.doesTableExist(client, "libraryDesign")){
            Table ld = dynamoDB.getTable("libraryDesign");
            Item book = new Item();
            book.withPrimaryKey("productID", 1).with("title", "Harry Potter")
                    .with("year", 11).with("nbPage", 854);
            ld.putItem(book);

            QuerySpec query1 = new QuerySpec().withKeyConditionExpression("productID = :Id")
                    .withValueMap(new ValueMap().withNumber(":Id", 1));

            Map<String, String> param = new HashMap();
            param.put("Hall", "Berry");
            param.put("Johny", "Depp");
            Item dvd = new Item();
            dvd.withPrimaryKey("productID", 2).with("title", "Mission Impossible").with("time", 90).with("actors", param);
            ld.putItem(dvd);

            System.out.println("<===== Question 4 =====>");

            for(Item i : ld.query(query1)) {
                System.out.println(i.toJSONPretty());
            }

            QuerySpec query2 = new QuerySpec().withKeyConditionExpression("productID = :Id")
                    .withValueMap(new ValueMap().withNumber(":Id", 2));

            for(Item i : ld.query(query2)) {
                System.out.println(i.toJSONPretty());
            }

            System.out.println("<===== Question 5 =====>");

            for (Item i : ld.scan()){
                System.out.println(i.toJSONPretty());
            }

            System.out.println("<===== Question 6 =====>");

            long time = System.currentTimeMillis();
            for (;compt < 2000; compt++){
                Item prout = new Item().withPrimaryKey("productID", compt).with("try", compt);
                ld.putItem(prout);
                System.out.println(compt+": "+((new Double((double)1/((System.currentTimeMillis()-time)/1000.0))))
                        .toString()+"/s");
                time=System.currentTimeMillis();
            }

            System.out.println("\nJob's done !");
>>>>>>> c87e89087e2422f840744d9782cdd931ddbc1822
        }

        List<KeySchemaElement> mesclefs = new ArrayList<KeySchemaElement>();
        mesclefs.add(new KeySchemaElement("primary", KeyType.HASH));
        mesclefs.add(new KeySchemaElement("secondary", KeyType.RANGE));

        List<AttributeDefinition> mesattributs = new ArrayList<AttributeDefinition>();
        mesattributs.add(new AttributeDefinition("primary", "S"));
        mesattributs.add(new AttributeDefinition("secondary", "S"));

        CreateTableRequest t = new CreateTableRequest()
                .withTableName("DoubleLibrary")
                .withKeySchema(mesclefs)
                .withAttributeDefinitions(mesattributs)
                .withProvisionedThroughput(new ProvisionedThroughput(2L, 2L));
        client.createTable(t);

    }

}
