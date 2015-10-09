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
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.util.Tables;
import com.amazonaws.services.rds.model.SourceType;
import com.sun.xml.internal.bind.v2.schemagen.xmlschema.AttributeType;

import java.util.ArrayList;
import java.util.List;


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

        if(Tables.doesTableExist(client, "DoubleLibrary")){
            Table dl = dynamoDB.getTable("DoubleLibrary");
            client.deleteTable("DoubleLibrary");
            try {
                dl.waitForDelete();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

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
