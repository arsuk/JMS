import java.io.BufferedReader;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.Enumeration;
import java.util.Map;
import java.util.Date;
import java.text.SimpleDateFormat;

import javax.naming.*;  
import javax.jms.*;  

import org.apache.activemq.jms.pool.PooledConnectionFactory;

public class MySenderPooled implements Runnable { 
	
    private static Properties props=new Properties();
	private static InitialContext ctx;
	private static PooledConnectionFactory pooledConnectionFactory;

    static MyArgs arg;
    SimpleDateFormat ft = new SimpleDateFormat ("hh:mm:ss"); 
    static QueueConnectionFactory factory=null;
    //static QueueConnection con=null;
       
    public static void main(String[] args) {
        arg=new MyArgs(args);
        //Create and start connection  

       try {
            props.load(new FileInputStream("jndi.sender.properties"));
            ctx=new InitialContext(props);
            
			factory = (QueueConnectionFactory)ctx.lookup("queueConnectionFactory");          
 
        } catch (NamingException e) {
        	System.out.println("Connection Factory: "+e);
            System.exit(1);
		} catch (Exception ie) {
            System.out.println("Properties: "+ie);
            System.exit(1);
		}  

        pooledConnectionFactory = new PooledConnectionFactory();
        pooledConnectionFactory.setConnectionFactory(factory);
        pooledConnectionFactory.setMaxConnections(10);
        pooledConnectionFactory.setMaximumActiveSessionPerConnection(5);
        pooledConnectionFactory.setBlockIfSessionPoolIsFull(true);
        pooledConnectionFactory.setBlockIfSessionPoolIsFullTimeout(10);
        pooledConnectionFactory.setIdleTimeout(60000);
        pooledConnectionFactory.start();

        int threads=1;
        String s=props.getProperty("threads");
        if (s!=null) try { threads=Integer.parseInt(s);} catch (Exception e) {};
 
        for (int i=0;i<threads;i++) {
        	Thread t = new Thread(new MySenderPooled());

        	t.start();
        }
    };
    
    public void run() {
 
        String name=getComputerName();
        int delay=0;        
        try{delay=Integer.parseInt(arg.getarg(0,"0"));}
           catch(Exception e) {System.out.println("Bad delay");System.exit(1);}
        String qName=arg.getarg(1,"myQueue");
        String persistent=props.getProperty("persistent");
        if (persistent==null) persistent=arg.getenv("ACTIVEMQ_PERSISTENT","no").toLowerCase();

        while (true)  
        try {
        	          
            BufferedReader b=new BufferedReader(new InputStreamReader(System.in));  
            while(true)  
            {  
            	
                System.out.println("Enter Msg, end to terminate:");  
                String s=b.readLine();
                int cnt=1;
                int mcnt=1;
                if (s.equals("end"))  
                    break;
                String split[]=s.split(",",2);
                if (split.length>1) try {
                    cnt=Integer.parseInt(split[0]);
                    s=split[1];
                    split=s.split(",",2);
                    if (split.length>1) {
                        mcnt=Integer.parseInt(split[0]);
                        s=split[1];
                    }
                } catch (NumberFormatException nf) {};
 
                long nanos=System.nanoTime(); 
                for (int i=0;i<cnt;i++) {

                    long ctimeNanos=System.nanoTime(); 
                    
                  	//1) create connection
                    QueueConnection con = pooledConnectionFactory.createQueueConnection();
            		con.start();
            		//2) create queue session  
                    QueueSession ses=con.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);  
                    //3) get the Queue object  
                    Queue t=(Queue)ctx.lookup(qName);  
                    //4)create QueueSender object         
                    QueueSender sender=ses.createSender(t);
                    if (persistent.startsWith("y") ) {
                         sender.setDeliveryMode(DeliveryMode.PERSISTENT);
                    System.out.println("Persistent mode");
                    } else
                        sender.setDeliveryMode(DeliveryMode.NON_PERSISTENT); 
                    //5) create TextMessage object  
                    TextMessage msg=ses.createTextMessage();

                    System.out.println("Connected "+i+" "+(System.nanoTime()-ctimeNanos)/1000000+" ms");

                    //6) send message
                    for (int j=0;j<mcnt;j++) { 
                        if (cnt>1 || mcnt>1)    // Include count in text
                            msg.setText(((i+1)*(j+1))+","+ft.format(new Date())+","+s);
                        else    // One single message
                            msg.setText(ft.format(new Date())+","+s);
                        msg.setStringProperty("Signature",name);    // Include a header element - the system name
                        sender.send(msg); // send message

                        // Check if delay after send wanted
                        if (delay>0) try {
                         Thread.sleep(delay);  
                        } catch (Exception e) {System.out.println("Delay error "+e);};
                    }
                    //8) connection close  
                    con.close();
                }
                System.out.println("Message successfully sent ["+Thread.currentThread().getId()+"] "+cnt+" in "
                        +(System.nanoTime()-nanos)/1000000+"ms");
            }
            b.close();
            System.exit(0);
        }catch(JMSException e){
            int secs=10;
            System.out.println(e);
            System.out.println("Retry in "+secs+"s"); 
            try {Thread.sleep(secs*1000);} catch (InterruptedException ie) {};
        }catch(Exception e){
            //System.out.println(e);
			e.printStackTrace(); 
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

