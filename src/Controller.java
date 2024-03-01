/*

    The Controller class is responsible to monitor the stream arrival rate and service rate.
    The thread will control the speed of the StreamGenerator class based on the service rate.
    It will ensure that the HYBRIDJOIN algorithm should not be underload and there should
    not be an unnecessary backlog in the stream buffer.

    This class also implements multi-threading to ensure that the StreamGenerator and
    HybridJoin classes are running in parallel.

*/

public class Controller

{

    //  Driver function.

    public static void main(String[] args)
    
    {

        try
        {
            int batchSize=10;   //  Size of the batch for processing.
            int batchSpeed=1000;    //  Speed of the batch processing (delay).
            
            //  Creating instances of the StreamGenerator and HybridJoin classes.
            
            HybridJoin hybridJoin=new HybridJoin();
            StreamGenerator streamGenerator=new StreamGenerator(hybridJoin, batchSize, batchSpeed);

            //  Creating threads for the StreamGenerator and HybridJoin classes.

            Thread streamGeneratorThread=new Thread(streamGenerator);
            Thread hybridJoinThread=new Thread(hybridJoin);

            //  Starting the threads.

            streamGeneratorThread.start();
            hybridJoinThread.start();

            //  Waiting for the threads to finish.

            streamGeneratorThread.join();
            hybridJoinThread.join();
        }
        catch(Exception Error)
        {
            Error.printStackTrace();
        }

    }

}
