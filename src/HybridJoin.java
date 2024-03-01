import java.util.*;
import java.sql.*;

/*  The HybridJoin class is responsible to perform the HYBRIDJOIN algorithm based on
    the Hybrid join (METHOD=4) approach discussed in the documentation for the
    Db2 11 for z/OS enterprise data server for IBM Z.

    This HYBRIDJOIN algorithm applies only to an inner join, and requires an index on
    the join column of the inner table. The method requires obtaining Record Identifiers (RID)
    in the order needed to use list prefetch.

    This implementation performs the following steps:

    1.  Scans the outer table (OUTER).
    2.  Joins the outer table with Record Identifiers (RID) from the index on the inner table.
        The result is the Phase 1 intermediate table. The index of the inner table is scanned
        for every row of the outer table.
    3.  Sorts the data in the outer table and the Record Identifiers (RID), creating a sorted
        Record Identifier (RID) list and the Phase 2 intermediate table. The sort is indicated
        by a value of Y in column SORTN_JOIN of the plan table. If the index on the inner table
        is a well-clustered index, the algorithm can skip this sort; the value in SORTN_JOIN is then N.
    4.  Retrieves the data from the inner table, using list prefetch.
    5.  Concatenates the data from the inner table and the Phase 2 intermediate table to create the
        final composite table.

    Reference:  https://www.ibm.com/docs/en/db2-for-zos/11?topic=operations-hybrid-join-method4

*/

public class HybridJoin implements Runnable

{

    public Map<Integer, LinkedList<SortEntry>> multiHashTable;  //  Creating a multi-hash table to store the batch entries.

    //  Constructor function.

    public HybridJoin()
    
    {

        this.multiHashTable=new HashMap<>();

    }

    //  Class to store the batch entries.

    public static class SortEntry
    
    {

        public final int customerID;
        public final int productID;

        //  Constructor function.

        public SortEntry(int productID, int customerID)
        
        {

            this.customerID=customerID;
            this.productID=productID;

        }

        //  Function to return the customer identifier.

        public int getCustomerID()
        
        {

            return customerID;

        }

        //  Function to return the product identifier.

        public int getProductID()
        
        {

            return productID;

        }

    }

    //  Driver function.

    @Override
    public void run() {}

    //  Function to process the batch entries.

    public void processBatchEntries(List<SortEntry> batchEntries, Connection connection) throws SQLException
    
    {

        //  Iterating over the batch entries.

        for(SortEntry entry : batchEntries)
        {

            int productID=entry.getProductID();
            int customerID=entry.getCustomerID();
            addToMultiHashTable(productID, entry);  //  Adding the batch entry to the multi-hash table.
            int timeID=performTimeDimensionJoin(connection, productID); //  Performing the `Time_Dimension` join.
            int storeID=performStoreDimensionJoin(connection, productID);   //  Performing the `Store_Dimension` join.
            updateSalesFact(connection, productID, customerID, timeID, storeID);    //  Updating the fact table.
        }

    }

    //  Function to add the batch entry to the multi-hash table.

    public void addToMultiHashTable(int productID, SortEntry entry)
    
    {

        if(multiHashTable.containsKey(productID))   //  Checking if the product identifier is already present in the multi-hash table or not.
        {
            multiHashTable.get(productID).add(entry);   //  Adding the batch entry to the multi-hash table.
        }
        else
        {
            LinkedList<SortEntry> newList=new LinkedList<>();   //  Creating a new linked list.
            newList.add(entry); //  Adding the batch entry to the linked list.
            multiHashTable.put(productID, newList); //  Adding the linked list to the multi-hash table.
        }

    }

    //  Function to perform the `Time_Dimension` join.

    public static int performTimeDimensionJoin(Connection connection, int productID) throws SQLException
    
    {

        PreparedStatement joinStatementTime=connection.prepareStatement(
            "SELECT Time_ID FROM Time_Dimension WHERE productID=?"
        );  //  Query to filter the table based on the product identifier.
        joinStatementTime.setInt(1, productID); //  Setting the product identifier.
        ResultSet resultSetTime=joinStatementTime.executeQuery();   //  Executing the query.
        int timeID=0;
        if(resultSetTime.next())    //  Checking if the result set is empty or not.
        {
            timeID=resultSetTime.getInt("Time_ID"); //  Fetching the time identifier.
        }
        resultSetTime.close();  //  Closing the result set.
        joinStatementTime.close();  //  Closing the prepared statement.

        return timeID;

    }

    //  Function to perform the `Store_Dimension` join.

    public static int performStoreDimensionJoin(Connection connection, int productID) throws SQLException
    
    {

        PreparedStatement joinStatementStore=connection.prepareStatement(
            "SELECT storeID FROM Store_Dimension WHERE productID=?"
        );  //  Query to filter the table based on the product identifier.
        joinStatementStore.setInt(1, productID);    //  Setting the product identifier.
        ResultSet resultSetStore=joinStatementStore.executeQuery(); //  Executing the query.
        int storeID=0;
        if(resultSetStore.next())   //  Checking if the result set is empty or not.
        {
            storeID=resultSetStore.getInt("storeID");   //  Fetching the store identifier.
        }
        resultSetStore.close(); //  Closing the result set.
        joinStatementStore.close(); //  Closing the prepared statement.

        return storeID;

    }

    //  Function to update the `Sales_Fact` table based on the batch entries.

    public static void updateSalesFact(Connection connection, int productID, int customerID, int timeID, int storeID) throws SQLException
    
    {

        PreparedStatement preparedStatement=connection.prepareStatement(
            "INSERT INTO Sales_Fact (productID, CustomerID, Time_ID, storeID) VALUES (?, ?, ?, ?)"
        );  //  Query to insert the batch entry into the table.

        //  Setting the values for the query.

        preparedStatement.setInt(1, productID);
        preparedStatement.setInt(2, customerID);
        preparedStatement.setInt(3, timeID);
        preparedStatement.setInt(4, storeID);

        preparedStatement.executeUpdate();  //  Executing the query.
        String query="SELECT * FROM Sales_Fact WHERE productID=? AND CustomerID=? AND Time_ID=? AND storeID=?"; //  Query to fetch the inserted entry.
        try(PreparedStatement selectStatement=connection.prepareStatement(query))
        {

            //  Setting the values for the query.

            selectStatement.setInt(1, productID);
            selectStatement.setInt(2, customerID);
            selectStatement.setInt(3, timeID);
            selectStatement.setInt(4, storeID);

            ResultSet resultSet=selectStatement.executeQuery(); //  Executing the query.

            //  Iterating over the result set.

            while(resultSet.next())
            {

                //  Retrieving the inserted entry from the result set.

                int insertedProductID=resultSet.getInt("productID");
                int insertedCustomerID=resultSet.getInt("CustomerID");
                int insertedTimeID=resultSet.getInt("Time_ID");
                int insertedStoreID=resultSet.getInt("storeID");
                double insertedTotalSale=resultSet.getDouble("Total_Sale");

                //  Streaming the inserted entry to the console.

                System.out.printf("| %-10s | %-12s | %-8s | %-8s | %-10s |\n", "Product ID", "Customer ID", "Time ID", "Store ID", "Total Sale");
                System.out.println("+------------+--------------+---------+---------+------------+");
                System.out.printf("| %-10d | %-12d | %-8d | %-8d | %-10.2f |\n", insertedProductID, insertedCustomerID, insertedTimeID, insertedStoreID, insertedTotalSale);

            }
        }
        preparedStatement.close();  //  Closing the prepared statement.

    }

}