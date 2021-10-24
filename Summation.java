import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;


public class Summation {
	
	public static String Table_Name = "GameTable";
	
	public static void main(String [] args) throws Exception {
		Configuration config = HBaseConfiguration.create();
		
		@SuppressWarnings({"resource", "deprecation"})
		
		HTable htable = new HTable(config, Table_Name);
		
		Scan sc = new Scan();
		sc.addColumn(Bytes.toBytes("Rating"), Bytes.toBytes("helpful_votes"));
		double sum = 0;
		double count = 0;
		
		ResultScanner rsc = htable.getScanner(sc);
		
			for (Result rr = rsc.next(); rr != null ; rr = rsc.next()){
			int num = Bytes.toInt(rr.value());
			sum = sum + num;
			count++;
			}
			System.out.println("Count: "+ count);
			System.out.println("Sum: " + sum);
			System.out.println("Average: " + sum/count);
		
	}

}
