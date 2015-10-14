package ec2;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.util.Tables;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EC2Use {

    public static final String TABLE_NAME = "products";
    public static final String PRIMARY_KEY = "productID";
    public static final String SECONDARY_INDEX = "title-index";

    public static void main(String[] args) {

        InstanceProfileCredentialsProvider instanceProfileCredentialsProvider = new InstanceProfileCredentialsProvider();

        AWSCredentials credentials1 = instanceProfileCredentialsProvider.getCredentials();
        System.out.println(credentials1.getAWSAccessKeyId() + " " + credentials1.getAWSSecretKey());

        //  Init db connection
        AmazonDynamoDB amazonDynamoDBClient = getAmazonDynamoDBClient(instanceProfileCredentialsProvider);

        DynamoDB dynamoDB = new DynamoDB(amazonDynamoDBClient);

        // build tables and indexes
        buildDBTables(TABLE_NAME, amazonDynamoDBClient, dynamoDB);

        //  Retrieve table object
        Table productsTable = dynamoDB.getTable(TABLE_NAME);

        //	Create basic items
        createBasicItems(productsTable);

        //	Display table content by query and scan
        checkBasicItemsByQuery(productsTable);
        checkBasicItemsByScan(TABLE_NAME, amazonDynamoDBClient);

        //	Retrieve number of random items to build
        int numberOrWriteToDo = (args.length > 0) ? Integer.parseInt(args[0]) : 0;

        //	Build given number of random items
        writeRandomItems(productsTable, numberOrWriteToDo);

        //	Scan with and without consistency
        scanWithConsistency(TABLE_NAME, amazonDynamoDBClient);
        scanWithoutConsistency(TABLE_NAME, amazonDynamoDBClient);

        //	Try a query with a secondary index (not primary key)
        queryItemsWithSecondIndex(productsTable);

        boolean deleteTable = (args.length > 1) ? Boolean.parseBoolean(args[1]) : false;
        if (deleteTable) deleteTable(dynamoDB, TABLE_NAME);
    }

    private static void buildDBTables(String tableName, AmazonDynamoDB amazonDynamoDBClient, DynamoDB dynamoDB) {
        System.out.println("\n+ Checking if table '" + tableName + "' exists ...");

        if (Tables.doesTableExist(amazonDynamoDBClient, tableName)) {
            System.out.println("\tTable " + tableName + " found !");
        } else {
            System.out.println("\tTable " + tableName + " not found.\n\tBuilding table " + tableName + " ...");

            //	List all key (only one primary key)
            List<KeySchemaElement> keySchemaElements = new ArrayList<KeySchemaElement>();
            KeySchemaElement primaryKey = new KeySchemaElement(PRIMARY_KEY, KeyType.HASH);
            keySchemaElements.add(primaryKey);

            //	List cols / attributes of table
            //	=> 	Build a table with PRIMARY_KEY col of type number and
            //		title col of type string
            List<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
            attributeDefinitions.add(new AttributeDefinition().withAttributeName(PRIMARY_KEY).withAttributeType("N"));
            attributeDefinitions.add(new AttributeDefinition().withAttributeName("title").withAttributeType("S"));

            //	Build a secondary index with read/write limit set to 1 per second
            //	and a projection on all attributes
            GlobalSecondaryIndex titleIndex = new GlobalSecondaryIndex().withIndexName(SECONDARY_INDEX)
                    .withProvisionedThroughput(
                            new ProvisionedThroughput()
                                    .withReadCapacityUnits(1L)
                                    .withWriteCapacityUnits(1L)

                    )
                    .withProjection(new Projection().withProjectionType(ProjectionType.ALL));

            //	SECONDARY_INDEX will influence title col only (not all attributes)
            List<KeySchemaElement> titleIndexSchema = new ArrayList<KeySchemaElement>();
            titleIndexSchema.add(
                    new KeySchemaElement()
                            .withAttributeName("title")
                            .withKeyType(KeyType.HASH)
            );

            //	Set the secondary key schema at its index
            titleIndex.setKeySchema(titleIndexSchema);

            //	Create table with read/write limit set to 1 per second and a secondary index
            CreateTableRequest createTableRequest = new CreateTableRequest()
                    .withTableName(TABLE_NAME)
                    .withKeySchema(keySchemaElements)
                    .withAttributeDefinitions(attributeDefinitions)
                    .withProvisionedThroughput(
                            new ProvisionedThroughput()
                                    .withReadCapacityUnits(1L)
                                    .withWriteCapacityUnits(1L)
                    )
                    .withGlobalSecondaryIndexes(titleIndex);

            //	Send request
            //	!IMPORTANT! after this line, the table can or can not be created
            //	in fact, this is just a call that will be triggered "some day"
            //	we must ensure that table and related indexes are created
            //	with waitForAllActiveOrDelete method
            amazonDynamoDBClient.createTable(createTableRequest);

            //  Retrieve table object
            Table productsTable = dynamoDB.getTable(tableName);

            //	Ensure that both tables and indexes are properly built and loaded
            try {
                System.out.println("\n\t\tWaiting for " + tableName
                        + " to be built ...this may take a while...");
                productsTable.waitForAllActiveOrDelete();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println("\t\t!! => Table" + tableName + " built !");
        }
    }

    private static void queryItemsWithSecondIndex(Table productsTable) {
        Index index = productsTable.getIndex(SECONDARY_INDEX);

        QuerySpec checkDvdQueryWithSecondIndex = new QuerySpec()
                .withKeyConditionExpression("#ti = :t")
                .withNameMap(new NameMap().with("#ti", "title"))
                .withValueMap(new ValueMap().withString(":t", "Pouetman-le film"));

        ItemCollection<QueryOutcome> resultDvdQueryWithSecondIndex = index.query(checkDvdQueryWithSecondIndex);
        for (Item currentItem : resultDvdQueryWithSecondIndex) {
            System.out.println("\n+ Item dvd found :\n" + currentItem.toJSONPretty());
        }
    }

    private static void scan(String tableName, AmazonDynamoDB amazonDynamoDBClient, boolean withConsistency) {
        if (withConsistency) {
            System.out.println("\n+ Scanning with consistency ...");
        } else {
            System.out.println("\n+ Scanning without consistency ...");
        }

        long initTimestamp = System.currentTimeMillis();

        ScanRequest scanRequest = new ScanRequest().withTableName(tableName).withConsistentRead(withConsistency);
        ScanResult scanResult = amazonDynamoDBClient.scan(scanRequest);

        long finalTimestamp = System.currentTimeMillis();
        long elapsedTime = finalTimestamp - initTimestamp;

        System.out.println("\tElapsed time : " + elapsedTime + "ms");
    }

    private static void scanWithoutConsistency(String tableName, AmazonDynamoDB amazonDynamoDBClient) {
        scan(tableName, amazonDynamoDBClient, false);
    }

    private static void scanWithConsistency(String tableName, AmazonDynamoDB amazonDynamoDBClient) {
        scan(tableName, amazonDynamoDBClient, true);
    }

    private static void writeRandomItems(Table productsTable, int numberOrWriteToDo) {
        long initTimestamp = System.currentTimeMillis();
        for (int i = 0; i < numberOrWriteToDo; i++) {
            Item itemRandom = new Item();
            itemRandom = itemRandom.withPrimaryKey(PRIMARY_KEY, 3 + i)
                    .with("title", "RandomItemN°" + (i + 3));

            productsTable.putItem(itemRandom);
        }
        long finalTimestamp = System.currentTimeMillis();
        long elapsedTime = finalTimestamp - initTimestamp;

        System.out.println(numberOrWriteToDo + "write operations asked (ASKED NOT DONE) in " + elapsedTime);
    }

    private static void checkBasicItemsByScan(String tableName, AmazonDynamoDB amazonDynamoDBClient) {
        //  Scan request / <=> Select * ?
        System.out.println("\n+ Scan result :");
        ScanRequest scanRequest = new ScanRequest().withTableName(tableName);
        ScanResult scanResult = amazonDynamoDBClient.scan(scanRequest);
        for (Map<String, AttributeValue> item : scanResult.getItems()) {
            System.out.println(item);
        }
    }

    private static void checkBasicItemsByQuery(Table productsTable) {
        //  Check if items were added with queries
        QuerySpec checkBookQuery = new QuerySpec()
                .withKeyConditionExpression(PRIMARY_KEY + " = :pID")
                .withValueMap(new ValueMap().withNumber(":pID", 1));

        ItemCollection<QueryOutcome> resultsBookQuery = productsTable.query(checkBookQuery);
        for (Item currentItem : resultsBookQuery) {
            System.out.println("\n+ Item book found :\n" + currentItem.toJSONPretty());
        }

        //  Check if items were added with queries
        QuerySpec checkDvdQuery = new QuerySpec()
                .withKeyConditionExpression(PRIMARY_KEY + " = :pID")
                .withValueMap(new ValueMap().withNumber(":pID", 2));

        ItemCollection<QueryOutcome> resultDvdQuery = productsTable.query(checkDvdQuery);
        for (Item currentItem : resultDvdQuery) {
            System.out.println("\n+ Item dvd found :\n" + currentItem.toJSONPretty());
        }
    }

    private static void createBasicItems(Table productsTable) {
        //  Create and add a book
        Item book = new Item();
        book = book.withPrimaryKey(PRIMARY_KEY, 1)
                .with("title", "pouetman")
                .with("year of publication", 2006)
                .with("number of pages", 69);

        productsTable.putItem(book);

        //  Create and add a dvd
        Item dvd = new Item();
        dvd = dvd.withPrimaryKey(PRIMARY_KEY, 2)
                .with("title", "Pouetman-le film")
                .with("year of publication", 2006)
                .with("duration", 169);

        productsTable.putItem(dvd);

        System.out.println("\n+ Item book and dvd added");
    }

    private static AmazonDynamoDB getAmazonDynamoDBClient(AWSCredentialsProvider awsCredentialsProvider) {
        AmazonDynamoDB amazonDynamoDBClient = new AmazonDynamoDBClient(awsCredentialsProvider);
        amazonDynamoDBClient.setRegion(com.amazonaws.regions.Region.getRegion(Regions.EU_CENTRAL_1));

        return amazonDynamoDBClient;
    }

    private static void deleteTable(DynamoDB dynamoDB, String tableName) {
        System.out.println("\n+ Deleting table asked ...");
        Table table = dynamoDB.getTable(tableName);

        try {
            System.out.println("\tIssuing DeleteTable request for " + tableName);
            table.delete();

            System.out.println("\tWaiting for " + tableName
                    + " to be deleted...this may take a while...");

            table.waitForDelete();
        } catch (Exception e) {
            System.err.println("DeleteTable request failed for " + tableName);
            System.err.println(e.getMessage());
        }

        System.out.println("\t\t!! => Table" + tableName + " deleted !");
    }

}
