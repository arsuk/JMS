import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.util.Properties;

import javax.jms.*;  
import javax.naming.InitialContext;  
  
public class MyReceiverListener implements MyMessageListener,ExceptionListener {

	static int sleepTime=0;
	static MyJMS myJMS;
	
    public static void main(String[] args) {

		MyArgs arg=new MyArgs(args);

        try {
            sleepTime=Integer.parseInt(arg.getarg(0,"0"));
        } catch(Exception e) {
            System.out.println("Bad delay");
            System.out.println("Usage: java -cp target/jms-example-0.1-SNAPSHOT.jar MyReceiver <sleep-time> <jndi-queue name>");
            System.out.println("All other options set in 'jndi.receiver.properties'");
            System.exit(1);
        };
        String qName=arg.getarg(1,"myQueue");

        Properties props=new Properties();
        try {
            props.load(new FileInputStream("jndi.receiver.properties"));
        } catch (Exception ie) {
            System.out.println("Properties: "+ie);
            System.out.println("Using classpath JNDI properties");
            props=null;
        }
        
    	try{
            sleepTime=Integer.parseInt(MyArgs.arg(args,0,"0"));
            
            props.setProperty("QueueName",qName);

    		myJMS = new MyJMS(props); 
    		
    		String tx=props.getProperty("Transacted");
    		if (tx==null) tx=arg.getenv("ACTIVEMQ_TX","no");
    		boolean transacted=tx.toLowerCase().trim().startsWith("y");

    		myJMS.setAutoAcknowledge(false);
    		myJMS.setTransacted(transacted);
    		myJMS.openConnection();
    		myJMS.createReceiver(qName);

    		MyReceiverListener listener=new MyReceiverListener();
            myJMS.setMessageListener(listener);
            myJMS.setExceptionListener(listener);
              
            System.out.println("Receiver is ready, waiting for messages...");  
            System.out.println("press Ctrl+c to shutdown...");  
            while(true) {                  
                Thread.sleep(1000);
            }  
        }catch(Exception e){System.out.println(e);
        e.printStackTrace();}  
    } 

    public void messageReceived() {
		String sig=myJMS.getStringProperty("Signature");
		if (sig==null) sig=""; else sig="("+sig+")";
	  
		System.out.println("Message received"+sig+": "+myJMS.getMessageText());

		try{ Thread.sleep(sleepTime*1000);} catch (Exception e) {};	// Dummy wait time to simulate work
	
		myJMS.acknowledgeMessage();

    }

	@Override
	public void onException(JMSException arg) {
		System.err.println("JMS Exception "+arg);
		System.exit(1);
	} 
  
} 
