import java.io.FileInputStream;
import java.util.Properties;
import java.util.Map;
import java.util.Date;
import java.util.Scanner;
import java.text.SimpleDateFormat;
 
//import javax.jms.*;  

public class MySender implements Runnable { 
	
    static MyJMS myJMS;
	static String name;
	static String qName;
	static String rqName="";
	static SimpleDateFormat ft = new SimpleDateFormat ("hh:mm:ss"); 
	
	static String helpStr="Commands:\n"+
			"receive [notext] [ms]    Receive messages with 'ms' timeout (0 forever)\n"+
			"send [msg]               Send a message or a <count> number of messages\n"+
			"count <n> [all]          Set number of messages to send with next send\n"+
			"                         or all following sends if 'all'\n"+
			"size <n>                 Set message size (text replicated to fill it)\n"+
			"delay <ms>               Delay between send and acknowledge/commit\n"+
			"deliverydelay <ms>       Broker 'schedulerSupport' delay property\n"+
			"deliverytime <ms>        Broker (Artemis only) current time + delay\n"+
			"persistent <true|false>  Set persistence on or off for the session\n"+
			"auto-ack <true|false>    Set auto-ack for receive on or off\n"+
			"manual-ack <true|false>  Set ack/commit for send after <delay> on or off\n"+
			"                         (must use ack/commit for transacted mesages)\n"+
			"transacted <true|false>  Set transation mode for the session\n"+
			"timeout <ms>             Send timeout in ms\n"+
			"queue <jndi name>        Change queue name\n"+
			"message [msg]            Change default message text\n"+
			"ack                      Acknowledge received messages\n"+
			"commit                   Commit session\n"+
			"recover                  Cancel received messages\n"+
			"rollback                 Rollback or cancel messages not yet committed\n"+
            "Remote commands: delay n, rollback, inflight, exit";
    
	public static void main(String[] args) {
	
		MyArgs arg=new MyArgs(args);
		
		name=getComputerName();
	       
		if (arg.getarg("-h") || arg.getarg("-?")) { 
            System.out.println("Usage: java -cp target/jms-example-0.1-SNAPSHOT.jar MySender <jndi-queue name>");
            System.out.println("All other options set in 'jndi.sender.properties'");
            System.exit(1);
        };

		qName=arg.getarg(0,"myQueue");
		System.out.println("JNDI queue name "+qName);

		//Create and start connection  
        Properties props=new Properties();
        try {
            props.load(new FileInputStream("jndi.sender.properties"));
        } catch (Exception ie) {
            System.out.println("Properties: "+ie);
            System.out.println("Using classpath JNDI properties");
            props=null;
        }
		
		String ps=props.getProperty("Persistent");
		if (ps==null) ps=arg.getenv("ACTIVEMQ_PERSISTENT","yes");
		ps=ps.toLowerCase().trim();
		boolean persistent=ps.startsWith("y")||ps.startsWith("t");	// yes or true

		String tx=props.getProperty("Transacted");
		if (tx==null) tx=arg.getenv("ACTIVEMQ_TX","no");
		tx=tx.toLowerCase().trim();
		boolean transacted=tx.startsWith("y")||tx.startsWith("t");	// yes or true

		String to=props.getProperty("SendTimeout");
		if (to==null) to=arg.getenv("ACTIVEMQ_TO","-1");
		int timeout=-1;
		try {timeout=Integer.parseInt(to);}
		catch (Exception e) {System.err.println("Bad timeout value");};
		
		myJMS=new MyJMS(props);
		myJMS.setTransacted(transacted);
		myJMS.setPersistent(persistent);
		myJMS.setSendTimeout(timeout);
		myJMS.openConnection();
        myJMS.createSender(qName);
		
		int threads=0;        
		try{threads=Integer.parseInt(arg.env("THREADS", "1"));}
		    catch(Exception e) {System.out.println("Bad threads");System.exit(1);}

		for (int i=0;i<threads;i++) {
			Thread t = new Thread(new MySender());	// Start threads sharing connection
			t.start();
		}

	}
	
	public void run() {

        Scanner scn=new Scanner(System.in);
        int count=1;
        boolean keepCountBool=false;
        int size=0;
        int delay=0;
        String str="Test message";
		boolean manualAck=false;
        
        while (true)  // Retry loop on connection errors
        try    {

            // Read message text  
            String cmd="";
            while(!cmd.trim().equals("exit"))  
            try {
           		System.out.println("Enter command (type help), exit to terminate:");
           		cmd=scn.next().trim();

                switch (cmd) {
                case "help":
                	System.out.println(helpStr);
                	scn.nextLine();	// skip to next line
                	break;
                case "exit":
                	break;
                case "count":
                	count=scn.nextInt();
                    String keep=scn.nextLine();	// skip to next line (get keep parameter)
                    keepCountBool=keep.trim().equals("all");
                    break;
                case "size":
                    size=scn.nextInt();
                	scn.nextLine();	// skip to next line
                    break;
                case "delay":
                	delay=scn.nextInt();
                	scn.nextLine();	// skip to next line
                	break;
                case "deliverydelay":
                	myJMS.setDeliveryDelay(scn.nextLong());	// JMS
                	scn.nextLine();	// skip to next line
                	break;
                case "deliverytime":
                	myJMS.setDeliveryTime(scn.nextLong());	// Artemis only
                	scn.nextLine();	// skip to next line
                	break;                	
                case "message":
                	scn.nextLine().trim();
                    break;
                case "queue":
	                qName=scn.next().trim();
	                myJMS.createSender(qName);
	                scn.nextLine();	// skip to next line
                    break;
                case "auto-ack":
	                myJMS.setAutoAcknowledge(scn.nextBoolean());
	                myJMS.openSession();
	                scn.nextLine();	// skip to next line
                    break;
                case "manual-ack":
    	            manualAck=scn.nextBoolean();
    	            scn.nextLine();	// skip to next line
                    break;
                case "persistent":
	                myJMS.setPersistent(scn.nextBoolean());
	                myJMS.openSession();
	                scn.nextLine();	// skip to next line
                    break;
                case "transacted":
	                myJMS.setTransacted(scn.nextBoolean());
	                myJMS.openSession();
	                scn.nextLine();	// skip to next line
                    break;
                case "timeout":
                	int sendTimeout=scn.nextInt();
                	myJMS.setSendTimeout(sendTimeout);
	                myJMS.openSession();
                	scn.nextLine();	// skip to next line
                	break;
                case "commit":
                case "ack":
	                myJMS.acknowledgeMessage();
	                scn.nextLine();	// skip to next line
                    break;
                case "rollback":
                case "recover":
	                myJMS.rollback();
	                scn.nextLine();	// skip to next line
                    break;
                case "send": {
                	String msg=scn.nextLine().trim();
                	if (msg.isEmpty()) msg=str;
                	while (msg.length()<size) {msg=msg+" "+str;}

	                //7) send message 
	                long nanos=System.nanoTime(); 
	                for (int i=0;i<count;i++) {
	                    String msgStr;
	                    if (count>1)
	                        msgStr=(i+1)+","+ft.format(new Date())+","+msg;
	                    else
	                        msgStr=ft.format(new Date())+","+msg;
	                    myJMS.setStringProperty("Signature",name);
	                    myJMS.sendMessage(msgStr);
	
	                    if ((i+1)%1000==0) System.out.println("Sent "+(i+1));
	
	                    // Check if delay after send wanted
	                    if (delay>0) try {
	                     Thread.sleep(delay);  
	                    } catch (Exception e) {System.out.println("Delay error "+e);};
	
	                    if (!manualAck)
	                    	myJMS.acknowledgeMessage();
	                }
	                System.out.println("Message(s) successfully sent "+count+" in "
	                        +(System.nanoTime()-nanos)/1000000+"ms");
                	if (!keepCountBool)
                		count=1;	// Set back to 1
	                break;
		            }
                case "receive": {
                	int timeout=1000;	// default timeout in ms
                    boolean noText=false;
                	String msg=scn.nextLine().trim();
                    if (msg.startsWith("notext")) {
                        noText=true;
                        msg=msg.substring(6).trim();
                    }
                	if (!msg.isEmpty()) try {
                		timeout=Integer.parseInt(msg);
                	} catch (Exception e) {};
                	String text="";
                	if (!rqName.equals(qName)) {
                		myJMS.createReceiver(qName);
                		rqName=qName;
                	}
	                long nanos=System.nanoTime();
	                int c=0;
	                while (text!=null) {
	                	myJMS.setReceiveTimeout(timeout);
	                	text=myJMS.receiveTextMessage();
	                	if (text!=null) {
                            if (!noText)
	                		    System.out.println(text);
	                		c++;
	                	}
	                }
	                System.out.println("Message(s) successfully received "+c+" in "
	                        +((System.nanoTime()-nanos)/1000000-timeout)+"ms");
                	break;
                	}
                default:
                	if (!cmd.isEmpty())
                		System.out.println("Unknown command");
                	scn.nextLine();	// skip to next line
                }
            } catch (java.util.InputMismatchException e) {
            	System.out.println("Bad input");
            	scn.nextLine();
            }

            // connection close  
            myJMS.closeConnection();
            System.exit(0);	// Killing any other tasks

        }catch(Exception e){
            System.out.println(e); 
            System.exit(1);
        } 

    } 

    private static String getComputerName()
    {
        Map<String, String> env = System.getenv();
        if (env.containsKey("COMPUTERNAME"))
            return env.get("COMPUTERNAME");
        else if (env.containsKey("HOSTNAME"))
            return env.get("HOSTNAME");
        else try {   
            String hostname = new String(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get("/etc/hostname"))).trim();    
            return hostname;
        } catch (Exception e) {
            return "Unknown Computer";
        }
    }
}  

