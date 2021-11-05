import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;
import java.util.Date;
import java.text.SimpleDateFormat;
  
public class MyReceiver { 

	static int sleepTime=0;
	static String qName;
	static String showp=MyArgs.env("SHOW_PROPS","no").toLowerCase();
 
	public static void main(String[] args) {

		MyArgs arg=new MyArgs(args);

        String usageText="Usage: java -cp target/jms-example-0.1-SNAPSHOT.jar MyReceiver <sleep-time> <jndi-queue name>\n"+
                         "All other options set in 'jndi.receiver.properties'";
        if (arg.getarg("-h") || arg.getarg("-?")) {
            System.out.println(usageText);
            System.exit(0);
        } 
        try {
            sleepTime=Integer.parseInt(arg.getarg(0,"0"));
        } catch(Exception e) {
            System.out.println("Bad delay");
            System.out.println(usageText);
            System.exit(1);
        };
        qName=arg.getarg(1,"myQueue");

        Properties props=new Properties();
        try {
            props.load(new FileInputStream("jndi.receiver.properties"));
        } catch (Exception ie) {
            System.out.println("Properties: "+ie);
            System.out.println("Using classpath JNDI properties");
            props=null;
        }
		
		String tx=props.getProperty("Transacted");
		if (tx==null) tx=arg.getenv("ACTIVEMQ_TX","no");
		boolean transacted=tx.toLowerCase().trim().startsWith("y");

		MyJMS myJMS=new MyJMS(props);
		myJMS.setAutoAcknowledge(false);
		myJMS.setTransacted(transacted);
		myJMS.openConnection();
		myJMS.createReceiver(qName);

		System.out.println("Receiver is ready, "+qName+" waiting for messages...");  
		System.out.println("press Ctrl+c to shutdown...");
		int count=0;
		while(true) {				  

			SimpleDateFormat ft = new SimpleDateFormat ("hh:mm:ss");  

			String text=myJMS.receiveTextMessage();
			
			if (text==null) {
				System.err.println("JMS error");
				System.exit(1);
			}

			String sig=myJMS.getStringProperty("Signature");
			if (sig==null) sig=""; else sig="("+sig+")";
			count++; 	  
			System.out.println("Message "+count+" "+sig+ft.format(new Date())+": "+text);
			// List all properties
			if (showp.startsWith("y")) {
				myJMS.printMessageProperties();
			}
			int delayIndex=text.indexOf("delay");
			int delayTime=0;	// delay within transaction simulating work
			if (delayIndex>=0) {
				String ms=text.substring(delayIndex+5);
				try {
					delayTime=new Scanner(ms).useDelimiter("\\D+").nextInt();
					Thread.sleep(delayTime);
				} catch (Exception e) {};
			}

            if (text.endsWith("rollback")) {
            	System.out.println("Rollback!");
            	myJMS.rollback();
            } else {
            	if (text.endsWith("inflight")&&!myJMS.getRedelivered()) {
            		System.out.println("Message left inflight");
                } else {
                	myJMS.acknowledgeMessage();
                }
            }
            if (delayTime>0)
            	System.out.println("Message ack time "+ft.format(new Date()));
            
			try{
                Thread.sleep(sleepTime);
            } catch (Exception e) {};	// Dummy wait time outside of transaction to force queueing 

            if (text.endsWith("exit")) {
                myJMS.closeConnection();
            	System.out.println("Exiting!");
            	System.exit(0);
            }
		}
	}  
} 
