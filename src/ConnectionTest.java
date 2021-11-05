import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.naming.InitialContext;

public class ConnectionTest {

	static String qName="myQueue";

	public static void main(String[] args) {
		
		String sl=MyArgs.arg(args, 0, "10");    // Sleep time between connections ms
		int ms=MyArgs.toInt(sl);

		String cl=MyArgs.arg(args, 1, "100");   // number connections
		int clc=MyArgs.toInt(cl);
		QueueConnection qc[]= new QueueConnection[clc];

		String rtimestr=MyArgs.arg(args, 2, "10000");   // Wait before retry
		int rtime=MyArgs.toInt(rtimestr);
		
		Properties props=new Properties();
		try {
			props.load(new FileInputStream("jndi.receiver.properties"));
		} catch (Exception e1) {
			System.out.println("Properties "+e1);
			System.exit(1);
		}

        System.out.println("Count "+clc+", sleep time "+ms);

		int cnt=0;
		while (true) // Loop creating and closing connections to check the AMQ configuration
		try{ 

			//1) Create and start connection
			InitialContext ctx=new InitialContext(props);  
			QueueConnectionFactory f=(QueueConnectionFactory)ctx.lookup("queueConnectionFactory");  
			QueueConnection con=f.createQueueConnection();  
			con.start();  
			//2) create Queue session  ars
			//QueueSession ses=con.createQueueSession(false, Session.CLIENT_ACKNOWLEDGE /*Session.AUTO_ACKNOWLEDGE*/);  
			//3) get the Queue object  
			//Queue t=(Queue)ctx.lookup(qName);
			//4)create QueueReceiver  
			//QueueReceiver receiver=ses.createReceiver(t);

			System.out.println("Connect "+cnt+" "+qName);  
						
			cnt++;

			int j=cnt%clc;
			qc[j]=con;
			
			if (j==0 && cnt>0) {
				for (int i=0;i<clc;i++)
						qc[i].close();
				System.out.println("Closed "+clc+" connections. Total count "+cnt+". Retry in "+rtime+"ms");
				try {Thread.sleep(rtime);} catch (InterruptedException ie) {};
			}				

			try {Thread.sleep(ms);} catch (InterruptedException ie) {};
			
		}catch(JMSException e){
			int rsecs=60;
			System.out.println(e);
			System.out.println("Waiting for "+rsecs+"s before exit"); 
			try {Thread.sleep(rsecs*1000);} catch (InterruptedException ie) {};
            System.exit(1);
		}catch(Exception e){
			System.out.println(e); 
			System.exit(2);
		} finally {
			try {
				for (int i=0;i<cnt;i++) qc[i].close();
			} catch (Exception e) {}
		}
	}

}
