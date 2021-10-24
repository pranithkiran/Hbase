import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileNotFoundException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InsertData extends Configured implements Tool{

	public String Table_Name = "GameTable";
    @SuppressWarnings("deprecation")
	@Override
    public int run(String[] argv) throws IOException {
        Configuration conf = HBaseConfiguration.create();        
        @SuppressWarnings("resource")
		HBaseAdmin admin=new HBaseAdmin(conf);        
        
        boolean isExists = admin.tableExists(Table_Name);
        
        if(isExists == false) {
	        //create table with column family
	        HTableDescriptor htb=new HTableDescriptor(Table_Name);
	        HColumnDescriptor InfoFamily = new HColumnDescriptor("Info");
	        HColumnDescriptor RatingFamily = new HColumnDescriptor("Rating");
	        HColumnDescriptor ReviewFamily = new HColumnDescriptor("Review");
	        
	        htb.addFamily(InfoFamily);
	        htb.addFamily(RatingFamily);
	        htb.addFamily(ReviewFamily);
	        admin.createTable(htb);
        }
        
        try {
    		@SuppressWarnings("resource")
			BufferedReader br = new BufferedReader(new FileReader("/home/cloudera/git/HBase_Demo/amazon_reviews_us_Video_Games_v1_00.tsv"));
    	    String line;
    	    
    	    int row_count=0;
    	    
    	    //iterate over every line of the input file
    	    while((line = br.readLine()) != null) {
    	    	
    	    	if(line.isEmpty())continue;
    	    	
    	    	row_count++;
    	    	
    	    	String[] lineArray = line.split("\t");
    	    	String marketplace = lineArray[0];
    	    	String verified_purchase = lineArray[11];
    	    	int star_rating = Integer.parseInt(lineArray[7]);
    	    	int helpful_votes = Integer.parseInt(lineArray[8]);
    	    	int total_votes = Integer.parseInt(lineArray[9]);
    	    	String customer_id = lineArray[1];
    	    	String review_id = lineArray[2];
    	    	String review_headline = lineArray[12];
    	    	String review_body = lineArray[13];
    	    	//int retweets = Integer.parseInt(lineArray[7]);
    	    	
    	    	//initialize a put with row key as customer id and review id
	            Put put = new Put(Bytes.toBytes(customer_id + "_" + review_id));
	            
	            //add column data one after one
	            put.add(Bytes.toBytes("Info"), Bytes.toBytes("marketplace"), Bytes.toBytes(marketplace));
	            put.add(Bytes.toBytes("Info"), Bytes.toBytes("verified_purchase"), Bytes.toBytes(verified_purchase));
	            
	            
	            put.add(Bytes.toBytes("Rating"), Bytes.toBytes("star_rating"), Bytes.toBytes(star_rating));
	            put.add(Bytes.toBytes("Rating"), Bytes.toBytes("helpful_votes"), Bytes.toBytes(helpful_votes));
	            put.add(Bytes.toBytes("Rating"), Bytes.toBytes("total_votes"), Bytes.toBytes(total_votes));
	            
	            
	            put.add(Bytes.toBytes("Review"), Bytes.toBytes("review_headline"), Bytes.toBytes(review_headline));
	            put.add(Bytes.toBytes("Review"), Bytes.toBytes("review_body"), Bytes.toBytes(review_body));
	            //put.add(Bytes.toBytes("Info"), Bytes.toBytes("Retweets"), Bytes.toBytes(retweets));
	            
	            //add the put in the table
    	    	HTable hTable = new HTable(conf, Table_Name);
    	    	hTable.put(put);
    	    	hTable.close();    
	    	}
    	    System.out.println("Inserted " + row_count + " Inserted");
    	    
	    } catch (FileNotFoundException e) {
	    	// TODO Auto-generated catch block
	    	e.printStackTrace();
	    } catch (IOException e) {
	    	// TODO Auto-generated catch block
	    	e.printStackTrace();
	    } 

      return 0;
   }
    
    public static void main(String[] argv) throws Exception {
        int ret = ToolRunner.run(new InsertData(), argv);
        System.exit(ret);
    }
}