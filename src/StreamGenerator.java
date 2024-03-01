import java.util.*;
import java.sql.*;
import java.io.FileReader;
import java.io.IOException;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

/*

    The StreamGenerator class is used to generate the data for the `Electronica_DW` data warehouse
    by simulating a data-processing stream. It is responsible for reading the data from the
    comma-separated values (CSV) files and inserting them into relevant dimension tables in the database.

    This class also utilises an instance of the HybridJoin class to join the dimension tables using the
    HYBRIDJOIN algorithm and create the fact table for the data warehouse.

*/

public class StreamGenerator implements Runnable

{

    private final HybridJoin hybridJoin;
    private final int batchSize;
    private final int batchSpeed;

    //  Creating dictionaries to store the data from the comma-separated values (CSV) files.

    private final static Map<String, Map<String, String>> transactionsDictionary=new HashMap<>();
    private final static Map<String, Map<String, String>> masterDataDictionary=new HashMap<>();

    //  Creating sets to store the unique identifiers of the dimension tables to avoid duplicate entries.

    private final static Set<String> processedSupplierIDs=new HashSet<>();
    private final static Set<String> processedProductIDs=new HashSet<>();
    private final static Set<String> processedCustomerIDs=new HashSet<>();
    private final static Set<String> processedStoreIDs=new HashSet<>();

    //  Constructor function.

    public StreamGenerator(HybridJoin hybridJoin, int batchSize, int batchSpeed)
    
    {

        this.hybridJoin=hybridJoin;
        this.batchSize=batchSize;
        this.batchSpeed=batchSpeed;

    }

    //  Driver function.

    @Override
    public void run()
    
    {

        processData();

    }

    //  Function to perform the data processing.

    public void processData()
    
    {

        String transactionsFile="data/transactions.csv";
        String masterDataFile="data/master_data.csv";
        readAndStoreData(transactionsFile, transactionsDictionary);
        readAndStoreData(masterDataFile, masterDataDictionary);

        //  Connecting to the database.

        try(Connection connection=DriverManager.getConnection("jdbc:mysql://localhost:3306/Electronica_DW", "root", "password"))
        {
            
            //  Processing the dimension tables.
            
            insertIntoSupplierDimension(connection, masterDataDictionary);
            insertIntoProductDimension(connection, masterDataDictionary);
            insertIntoCustomerDimension(connection, transactionsDictionary);
            insertIntoTimeDimension(connection, transactionsDictionary);
            insertIntoStoreDimension(connection, masterDataDictionary);

            //  Processing the fact table.

            System.out.println("- Processing the Sales_Fact table...");
            ResultSet outerResultSet=connection.createStatement().executeQuery(
                "SELECT * FROM Customer_Dimension"
            );  //  Outer relation for the HYBRIDJOIN algorithm.
            List<HybridJoin.SortEntry> batchEntries=new ArrayList<>();  //  Creating a list to store the batch entries.
            int totalRows=0;

            //  Iterating over the outer relation.

            while(outerResultSet.next())
            {
                int productID=outerResultSet.getInt("productID");   //  Retrieving the product identifier from the outer relation.
                int customerID=outerResultSet.getInt("CustomerID"); //  Retrieving the customer identifier from the outer relation.
                batchEntries.add(new HybridJoin.SortEntry(productID, customerID));  //  Adding the entry to the batch.
                if(batchEntries.size()>=batchSize)  //  Checking if the batch is full or not.
                {
                    totalRows+=batchEntries.size(); //  Incrementing the total number of rows processed.
                    System.out.println("\n- Processing batch...");
                    System.out.println("("+totalRows+" row(s) processed)\n");
                    processBatch(batchEntries, connection); //  Processing the batch.
                    batchEntries.clear();   //  Clearing the batch.
                    try
                    {
                        Thread.sleep(batchSpeed);   //  Delaying the processing of the next batch.
                    }
                    catch(InterruptedException Error)
                    {
                        Error.printStackTrace();
                    }
                }
            }
            if(!batchEntries.isEmpty()) //  Checking if there are any remaining entries in the batch.
            {
                totalRows+=batchEntries.size(); //  Incrementing the total number of rows processed.
                System.out.println("\n- Processing remaining batch...");
                System.out.println("("+totalRows+" row(s) processed)\n");
                processBatch(batchEntries, connection); //  Processing the remaining batch.
            }
            System.out.println("\n- Sales_Fact filled successfully!\n");
            System.out.println("- Electronica_DW created successfully!");
        }
        catch(SQLException Error)
        {
            Error.printStackTrace();
        }

    }

    //  Function to read and store the data from the comma-separated values (CSV) files into dictionaries.

    private static void readAndStoreData(String csvFile, Map<String, Map<String, String>> dataDictionary)

    {

        System.out.println("- Streaming data from "+csvFile+"...");
        try(CSVReader reader=new CSVReader(new FileReader(csvFile)))
        {
            List<String[]> rows=reader.readAll();   //  Reading the data from the comma-separated values (CSV) file.
            String[] header=rows.get(0);    //  Retrieving the header row.

            //  Iterating over the rows of the comma-separated values (CSV) file.

            for(int i=1; i<rows.size(); i++)
            {
                String[] row=rows.get(i);   //  Retrieving the current row.
                Map<String, String> rowData=new HashMap<>();    //  Creating a dictionary to store the data of the current row.
                
                //  Iterating over the columns of the current row.
                
                for(int j=0; j<header.length; j++)
                {
                    rowData.put(header[j], row[j]); //  Storing the data of the current column in the dictionary.
                }
                dataDictionary.put("Row "+i, rowData);  //  Storing the data of the current row in the dictionary.
            }
        }
        catch(IOException | CsvException Error)
        {
            Error.printStackTrace();
        }
        System.out.println("- "+csvFile+" processed successfully!\n");
    }

    //  Function to insert the data into the `Supplier_Dimension` table.

    private static void insertIntoSupplierDimension(Connection connection, Map<String, Map<String, String>> dataDictionary)

    {

        System.out.println("- Processing the Supplier_Dimension table...");
        int rowsProcessed=0;
        try
        {
            String tableName="Supplier_Dimension";
            String[] relevantColumns={"supplierID", "supplierName"};
            String sql="INSERT INTO "+tableName+"("+String.join(",", relevantColumns)+") VALUES (?, ?)";    //  Query to insert the data into the table.
            try(PreparedStatement preparedStatement=connection.prepareStatement(sql))
            {

                //  Iterating over the rows of the dictionary.

                for(Map<String, String> rowData : dataDictionary.values())
                {
                    String supplierID=rowData.get("supplierID");    //  Retrieving the supplier identifier from the dictionary.
                    if(!processedSupplierIDs.contains(supplierID))  //  Checking if the supplier identifier has already been processed or not.
                    {

                        //  Inserting the relevant data into the table.

                        preparedStatement.setString(1, supplierID);
                        preparedStatement.setString(2, rowData.get("supplierName"));
                        preparedStatement.addBatch();   //  Adding the entry to the batch.
                        processedSupplierIDs.add(supplierID);   //  Adding the supplier identifier to the set.
                        rowsProcessed++;
                    }
                }
                preparedStatement.executeBatch();   //  Executing the batch.
            }
            System.out.println("- Supplier_Dimension filled successfully!");
            System.out.println("("+rowsProcessed+" row(s) affected)\n");
        }
        catch(SQLException Error)
        {
            Error.printStackTrace();
        }

    }

    //  Function to insert the data into the `Product_Dimension` table.

    private static void insertIntoProductDimension(Connection connection, Map<String, Map<String, String>> dataDictionary)

    {

        System.out.println("- Processing the Product_Dimension table...");
        int rowsProcessed=0;
        try
        {
            String tableName="Product_Dimension";
            String[] relevantColumns={"productID", "productName", "productPrice", "supplierID"};
            String sql="INSERT INTO "+tableName+"("+String.join(",", relevantColumns)+") VALUES (?, ?, ?, ?)";  //  Query to insert the data into the table.
            try(PreparedStatement preparedStatement=connection.prepareStatement(sql))
            {

                //  Iterating over the rows of the dictionary.

                for(Map<String, String> rowData : dataDictionary.values())
                {
                    String productID=rowData.get("productID");  //  Retrieving the product identifier from the dictionary.
                    if(!processedProductIDs.contains(productID))    //  Checking if the product identifier has already been processed or not.
                    {

                        //  Inserting the relevant data into the table.

                        preparedStatement.setString(1, productID);
                        preparedStatement.setString(2, rowData.get("productName"));
                        preparedStatement.setString(3, rowData.get("productPrice"));
                        preparedStatement.setString(4, rowData.get("supplierID"));
                        preparedStatement.addBatch();   //  Adding the entry to the batch.
                        processedProductIDs.add(productID); //  Adding the product identifier to the set.
                        rowsProcessed++;
                    }
                }
                preparedStatement.executeBatch();   //  Executing the batch.
            }
            System.out.println("- Customer_Dimension filled successfully!");
            System.out.println("("+rowsProcessed+" row(s) affected)\n");
        }
        catch(SQLException Error)
        {
            Error.printStackTrace();
        }

    }

    //  Function to insert the data into the `Customer_Dimension` table.

    private static void insertIntoCustomerDimension(Connection connection, Map<String, Map<String, String>> dataDictionary)

    {

        System.out.println("- Processing the Customer_Dimension table...");
        int rowsProcessed=0;
        try
        {
            String tableName="Customer_Dimension";
            String[] relevantColumns={"CustomerID", "CustomerName", "Gender", "productID"};
            String sql="INSERT INTO "+tableName+"("+String.join(",", relevantColumns)+") VALUES (?, ?, ?, ?)";  //  Query to insert the data into the table.
            try(PreparedStatement preparedStatement=connection.prepareStatement(sql))
            {

                //  Iterating over the rows of the dictionary.

                for(Map<String, String> rowData : dataDictionary.values())
                {
                    String customerID=rowData.get("CustomerID");    //  Retrieving the customer identifier from the dictionary.
                    if(!processedCustomerIDs.contains(customerID))  //  Checking if the customer identifier has already been processed or not.
                    {

                        //  Inserting the relevant data into the table.

                        preparedStatement.setString(1, customerID);
                        preparedStatement.setString(2, rowData.get("CustomerName"));
                        preparedStatement.setString(3, rowData.get("Gender"));
                        preparedStatement.setString(4, rowData.get("ProductID"));
                        preparedStatement.addBatch();   //  Adding the entry to the batch.
                        processedCustomerIDs.add(customerID);   //  Adding the customer identifier to the set.
                        rowsProcessed++;
                    }
                }
                preparedStatement.executeBatch();   //  Executing the batch.
            }
            System.out.println("- Customer_Dimension filled successfully!");
            System.out.println("("+rowsProcessed+" row(s) affected)\n");
        }
        catch(SQLException Error)
        {
            Error.printStackTrace();
        }

    }

    //  Function to insert the data into the `Time_Dimension` table.

    private static void insertIntoTimeDimension(Connection connection, Map<String, Map<String, String>> dataDictionary)

    {

        System.out.println("- Processing the Time_Dimension table...");
        int rowsProcessed=0;
        try
        {
            String tableName="Time_Dimension";
            String[] relevantColumns={"`Order ID`", "`Order Date`", "`Quantity Ordered`", "productID"};
            String sql="INSERT INTO "+tableName+"("+String.join(",", relevantColumns)+") VALUES (?, ?, ?, ?)";  //  Query to insert the data into the table.
            try(PreparedStatement preparedStatement=connection.prepareStatement(sql))
            {

                //  Iterating over the rows of the dictionary.

                for(Map<String, String> rowData : dataDictionary.values())
                {

                    //  Inserting the relevant data into the table.

                    preparedStatement.setString(1, rowData.get("Order ID"));
                    preparedStatement.setString(2, rowData.get("Order Date"));
                    preparedStatement.setString(3, rowData.get("Quantity Ordered"));
                    preparedStatement.setString(4, rowData.get("ProductID"));
                    preparedStatement.addBatch();   //  Adding the entry to the batch.
                    rowsProcessed++;
                }
                try
                {
                    preparedStatement.executeBatch();   //  Executing the batch.
                }
                catch(BatchUpdateException Error)
                {

                    //  Ignoring the error caused by incorrect datetime values.

                    if(Error.getMessage().contains("Incorrect datetime value"))
                    {
                        System.out.println("- Ignoring incorrect datetime values...");
                    }
                    else
                    {
                        Error.printStackTrace();
                    }
                }
            }
            System.out.println("- Time_Dimension filled successfully!");
            System.out.println("("+rowsProcessed+" row(s) affected)\n");
        }
        catch(SQLException Error)
        {
            Error.printStackTrace();
        }

    }

    //  Function to insert the data into the `Store_Dimension` table.

    private static void insertIntoStoreDimension(Connection connection, Map<String, Map<String, String>> dataDictionary)

    {

        System.out.println("- Processing the Store_Dimension table...");
        int rowsProcessed=0;
        try
        {
            String tableName="Store_Dimension";
            String[] relevantColumns={"storeID", "storeName", "productID"};
            String sql="INSERT INTO "+tableName+"("+String.join(",", relevantColumns)+") VALUES (?, ?, ?)"; //  Query to insert the data into the table.
            try(PreparedStatement preparedStatement=connection.prepareStatement(sql))
            {

                //  Iterating over the rows of the dictionary.

                for(Map<String, String> rowData : dataDictionary.values())
                {
                    String storeID=rowData.get("storeID");  //  Retrieving the store identifier from the dictionary.
                    if(!processedStoreIDs.contains(storeID))    //  Checking if the store identifier has already been processed or not.
                    {

                        //  Inserting the relevant data into the table.

                        preparedStatement.setString(1, storeID);
                        preparedStatement.setString(2, rowData.get("storeName"));
                        preparedStatement.setString(3, rowData.get("productID"));
                        preparedStatement.addBatch();   //  Adding the entry to the batch.
                        processedStoreIDs.add(storeID); //  Adding the store identifier to the set.
                        rowsProcessed++;
                    }
                }
                preparedStatement.executeBatch();   //  Executing the batch.
            }
            System.out.println("- Store_Dimension filled successfully!");
            System.out.println("("+rowsProcessed+" row(s) affected)\n");
        }
        catch(SQLException Error)
        {
            Error.printStackTrace();
        }

    }

    //  Function to process the batch entries.

    private void processBatch(List<HybridJoin.SortEntry> batchEntries, Connection connection) throws SQLException
    
    {

        Collections.sort(batchEntries, Comparator.comparingInt(HybridJoin.SortEntry::getProductID));    //  Sorting the batch entries by the product identifier.
        hybridJoin.processBatchEntries(batchEntries, connection);   //  Joining the dimension tables using the HYBRIDJOIN algorithm.

    }
    
}