package connector;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.util.Tables;
import com.amazonaws.services.rds.model.SourceType;


/**
 * Created by remy on 09/10/15.
 */
public class AmazonConnector {
    public static String usernane = "Remy";

    public static void main(String [] args){
        ProfileCredentialsProvider provider = new ProfileCredentialsProvider(usernane);

        AWSCredentials credentials = provider.getCredentials();
        AmazonDynamoDBClient client = new AmazonDynamoDBClient(credentials);
        client.setRegion(Region.getRegion(Regions.EU_WEST_1));

        DynamoDB dynamoDB = new DynamoDB(client);

        if (Tables.doesTableExist(client, "libraryDesign")){
            Table ld = dynamoDB.getTable("libraryDesign");
            Item book = new Item();
            book.withPrimaryKey("productID", 1).with("title", "Harry Potter")
                    .with("year", 11).with("nbPage", 400);
            ld.putItem(book);

            QuerySpec query1 = new QuerySpec().withKeyConditionExpression("productID = :Id")
                    .withValueMap(new ValueMap().withNumber(":Id", 1));

            for(Item i : ld.query(query1)) {
                System.out.println(i.toJSONPretty());
            }

            System.out.println("Job's done !");
        }
    }

}
