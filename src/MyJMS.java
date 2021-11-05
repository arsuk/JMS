import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;

import javax.jms.*;  
import javax.naming.InitialContext;  
import javax.naming.NamingException;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ScheduledMessage;
import org.apache.activemq.transport.TransportListener;

public class MyJMS implements MessageListener, ExceptionListener, TransportListener { 

    private static final Boolean TRANSACTED = true;
    private static final Boolean NON_TRANSACTED = false;

	private int retryTime=10;
	private int receiveTimeout=0;
	private QueueConnectionFactory connectionFactory;
	private QueueSession session;
	private InitialContext context;
	private Queue sendQueue;
	private Queue receiveQueue;
	private QueueReceiver receiver;
    private QueueSender sender; 
	private Message recvMessage=null;
	private QueueConnection con=null;
	private TextMessage sendMessage;
	private boolean persistent=true;
	private boolean acknowledge=true;
	private boolean transacted=false;
    private boolean failover=false;
    private boolean redelivered=false;
	private MyMessageListener myListener;
    private String url;
    private int sendTimeout=-1;

	//
	// The order of JMS steps used for this class is:
	//0) Create factory using JNDI context (class initialisation)
	//2) create Queue session (openConnection)  
	//3) get the queue object (openConnection) 
	//4) create QueueSender (sendMessage)
	//	or
	//4) create QueueReceiver (receiveMessage)
	//
	//5) create TextMessage object (sendMessage)
	//6) send message  (sendMessage)
	//	or
	//5) receive message (receiveMessage)
	//
	// Connection closed in closeConnection
	//
	// Listener set with setMessageListener (alternative to calling receiveMessage)
	// 
	
	public MyJMS() {
		this(null);
	}
	
	public MyJMS(Properties props) {
		
		//0) Create factory using JNDI context 
		try {
			if (props==null) 
				context=new InitialContext();  // Use classpath jndi properties if any				
			else {
		        String conStr=props.getProperty("java.naming.provider.url");
		        failover=(conStr!=null && conStr.startsWith("failover"));	
				context=new InitialContext(props);  
			}
			connectionFactory=(QueueConnectionFactory)context.lookup("queueConnectionFactory");
		} catch (NamingException ne) {
	        System.err.println("Properties: "+ne);
	        System.exit(1);			
		};

        url=props.getProperty("java.naming.provider.url");
	}
	
	public void openConnection() {
		// Open connection with retry loop....
        boolean retry=true;
		while (retry) 
		try{
            System.out.println("Opening connection");
            retry=!failover; // Note that this retry loop is not needed when using 'failover' in the jndi uri
			//1) Create and start connection  
			con=connectionFactory.createQueueConnection();
			ActiveMQConnection acon=(ActiveMQConnection) con;
			acon.addTransportListener(this);
			con.start();  
			//2) create Queue session
			openSession();
			System.out.println("Connected");
			return;
		}catch(JMSException e){
 			closeConnection();
			int secs=retryTime;
			System.err.println(e);
			System.err.println("Retry in "+secs+"s"); 
            sleepSecs(secs);
		}catch(Exception e){
			System.err.println(e); 
			System.exit(1);
		}
	}
	
	public void openSession() {

		try{
			closeSession();
			//2) create Queue session
            if (transacted)
                if (acknowledge)
				    session=con.createQueueSession(TRANSACTED, Session.AUTO_ACKNOWLEDGE);
                else
                    session=con.createQueueSession(TRANSACTED, Session.CLIENT_ACKNOWLEDGE);
            else
			    if (acknowledge)
				    session=con.createQueueSession(NON_TRANSACTED, Session.AUTO_ACKNOWLEDGE);
			    else
				    session=con.createQueueSession(NON_TRANSACTED, Session.CLIENT_ACKNOWLEDGE);
            System.out.println("Open session");
            System.out.println("Auto Acknowledge: "+acknowledge);
            System.out.println("Transacted: "+transacted);
            if (sendTimeout>=0) {
            	System.out.println("Send timeout old: "+((ActiveMQConnection)con).getSendTimeout()+" new: "+sendTimeout);
            	((ActiveMQConnection)con).setSendTimeout(sendTimeout); 
            }
			if (sendQueue!=null) createSender();
			if (receiveQueue!=null) createReceiver();
			
			return;
 
		}catch(JMSException e){
 			closeConnection();
			int secs=retryTime;
			System.err.println(e);
			System.err.println("Retry in "+secs+"s"); 
            sleepSecs(secs);
		}catch(Exception e){
			System.err.println(e); 
			System.exit(1);
		}
	}

	public void closeSession() {
		try {
			if (sender!=null) sender.close();
			if (receiver!=null) receiver.close();
			if (session!=null) {
		        System.out.println("Close session");
				session.close();
			}
		} catch (JMSException e) {
            System.err.println("Closing session "+e);
		} finally {
			session=null;
            sender=null;
			receiver=null;
			recvMessage=null;
            sendMessage=null;
		}
	}

	public void closeConnection() {
        System.out.println("Close connection");
		try {
			closeSession();
			if (con!=null) con.close();
            con=null;
		} catch (JMSException e) {
            con=null;
            System.err.println("Closing connection "+e);
			return;
		}
		
	}
	
	public void createSender (String queueName) {

		//3) get the queue object
        try {
        	sendQueue=(Queue)context.lookup(queueName);
        } catch (Exception e) {
			System.err.println("JMS sender error "+e);
			return;
        }
        createSender();
	}
	
	private void createSender() {
		//4)create QueueSender object
		try {
			if (sender!=null) sender.close();
			sender=session.createSender(sendQueue);
			if (persistent) {
				sender.setDeliveryMode(DeliveryMode.PERSISTENT);
			} else
				sender.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		} catch (JMSException je) {
			System.err.println("JMS sender error "+je);
		}
	}

	public void createReceiver (String queueName) {

		//3) get the queue object
        try {
        	receiveQueue=(Queue)context.lookup(queueName);
        } catch (Exception e) {
			System.err.println("JMS receiver error "+e);
			return;
        }
        createReceiver();
	}
	
	private void createReceiver() {
		//4)create QueueSender object
		try {
			if (receiver!=null) receiver.close();
			receiver=session.createReceiver(receiveQueue);
		} catch (JMSException je) {
			System.err.println("JMS receiver error "+je);
		}
	}

	public void sendMessage(TextMessage msg) {
        if (con==null) {System.err.println("Connection closed"); return;};

		while (true)
		try {
			if (sender==null) {
				System.err.println("No Sender");
				return;
			}
            //System.out.println("send msg "+msg.getText()+" "+myQueue.toString());
			msg.setJMSCorrelationID(con.getClientID());
			sender.send(msg);
			//System.out.println("send msg id "+msg.getJMSMessageID());
			return;
		} catch (JMSException je) {
			System.err.println("JMS send error "+je);
            int secs=retryTime;
            System.out.println("Retry in "+secs+"s"); 
            sleepSecs(secs);
            if (!je.toString().contains("ResourceAllocationException")) {
                System.out.println("Reconnecting");
                closeConnection();
                openConnection();
            }
		}
	}
	
	public void sendMessage(String text) {

		try {
			if (sendMessage==null) {
				//5) create TextMessage object  
				sendMessage=session.createTextMessage();
			}
			sendMessage.setText(text);
		} catch (JMSException je) {
			System.err.println("JMS send text error "+je);
			return;
		};
		
		sendMessage(sendMessage);
	}
	
	public Message receiveMessage() {
        if (con==null) {System.err.println("Connection closed"); return null;};

		while (true)
		try {
			if (receiver==null) {
				System.err.println("No Receiver");
				return null;
			}
			recvMessage=receiver.receive(receiveTimeout);
			if (recvMessage!=null&&(redelivered=recvMessage.getJMSRedelivered())) {
				int redCnt=recvMessage.getIntProperty("JMSXDeliveryCount");
                System.out.println("Redelivered, delivery count: "+redCnt);
			}
			return recvMessage;
		} catch (JMSException je) {
			System.err.println("JMS receive error "+je);
	        int secs=retryTime;
	        System.out.println("Retry in "+secs+"s"); 
	        sleepSecs(secs);
	        closeConnection();
	        openConnection();
		}
	}
	
	public String receiveTextMessage() {
		receiveMessage();
		return getMessageText();
	}
	
	public String getMessageText() {
		if (recvMessage==null)
			return null;
		else
		try {
			if (recvMessage instanceof TextMessage) {
				TextMessage msg=(TextMessage)recvMessage; 
				return msg.getText();
			} else if (recvMessage instanceof BytesMessage){
				BytesMessage bytesMessage = (BytesMessage)recvMessage;
				byte[] b = new byte[(int) bytesMessage.getBodyLength()];
				bytesMessage.readBytes(b);
				return new String(b);
			} else {
				System.out.println("Unknown message type "+recvMessage.getClass());
				return recvMessage.toString();
			}
		} catch (JMSException je) {
			System.err.println("JMS get text error "+je);
			return null;
		} 
	}
	
	public void printMessageProperties() {
		if (recvMessage==null)
			return;
		try {
			Enumeration<String> pe = (Enumeration<String>) recvMessage.getPropertyNames();
			while (pe.hasMoreElements()) {
				String name = pe.nextElement();
				System.out.print(name + "=" + recvMessage.getStringProperty(name) + " ");
			}
		} catch (Exception e) {};	
		System.out.println();
	}
	
	public String getStringProperty (String key) {
		if (recvMessage==null)
			return null;
		else
		try {
			return recvMessage.getStringProperty(key);
		} catch (JMSException e) {
			System.err.println("JMS error "+e);
			return null;
		}
	}
	
	public int getIntProperty (String key) {
		if (recvMessage==null)
			return 0;
		else
		try {
			return recvMessage.getIntProperty(key);
		} catch (JMSException e) {
			System.err.println("JMS error "+e);
			return 0;
		}
	}

	public void setStringProperty (String key, String value) {
        if (con==null) {System.err.println("Connection closed"); return;};

		try {
		    if (sendMessage==null) {
		    	//5) create TextMessage object  
		    	sendMessage=session.createTextMessage();
		    }
		    sendMessage.setStringProperty(key,value);
		} catch (JMSException e) {
			return;
		}
	}
	
	public void setDeliveryDelay(long deliveryDelay) {
		try {
		    if (sendMessage==null) {
		    	//5) create TextMessage object  
		    	sendMessage=session.createTextMessage();
		    }
		    sendMessage.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, deliveryDelay);
		} catch (JMSException e) {
			return;
		}
	}
	
	public void setDeliveryTime(long deliveryTime) {
		// Artemis only - schedule at specific time instead of delay
		try {
		    if (sendMessage==null) {
		    	//5) create TextMessage object  
		    	sendMessage=session.createTextMessage();
		    }
		    sendMessage.setLongProperty("_AMQ_SCHED_DELIVERY", System.currentTimeMillis() + deliveryTime);
		} catch (JMSException e) {
			return;
		}
	}
	
	public void setMessageListener(MyMessageListener listener) {
        if (con==null) {System.err.println("Connection closed"); return;};

		myListener=listener;
        try {
			if (receiver==null) {
				System.err.println("No Receiver");
				return;
			}
			receiver.setMessageListener(this);
		} catch (JMSException e) {
			e.printStackTrace();
		}     		
	}
	
	public void setExceptionListener(ExceptionListener el) {
        if (con==null) {System.err.println("Connection closed"); return;};
        try {
        	con.setExceptionListener(el);
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	public void acknowledgeMessage() {
		try {
            if (transacted)
                session.commit();
            else {
                if (recvMessage!=null)
                     recvMessage.acknowledge();
                if (sendMessage!=null)
                    sendMessage.acknowledge();
            }
		} catch (JMSException je) {
			System.err.println("JMS acknowledge error "+je);
	        int secs=retryTime;
	        System.out.println("Retry in "+secs+"s"); 
	        try {Thread.sleep(secs*1000);} catch (InterruptedException ie) {};
	        this.closeConnection();
	        this.openConnection();
			return;
		}  
	}

    public void rollback() {
    	if (transacted)
		try {
            session.rollback();
		} catch (JMSException je) {
			System.err.println("JMS acknowledge error "+je);
	        int secs=retryTime;
	        System.out.println("Retry in "+secs+"s"); 
	        try {Thread.sleep(secs*1000);} catch (InterruptedException ie) {};
	        this.closeConnection();
	        this.openConnection();
			return;
		} else
			recoverMessages();
    }
	
	public void recoverMessages() {
		// This is a negative acknowledge on received messages
		if (session!=null)
		try {
			session.recover();
		} catch (JMSException je) {
			System.err.println("JMS recover error "+je);
			return;
		}  
	} 
	
	public void setPersistent(boolean persistent) {
		this.persistent=persistent;
	}
	
	public boolean getPersistent() {
		return persistent;
	}
	
	public void setAutoAcknowledge(boolean acknowledge) {
		this.acknowledge=acknowledge;
	}

	public boolean getAutoAcknowledge() {
		return acknowledge;
	}

	public void setSendTimeout(int sendTimeout) {
		this.sendTimeout=sendTimeout;
	}

	public int getSendTimeout() {
		return sendTimeout;
	}

	public void setReceiveTimeout(int receiveTimeout) {
		this.receiveTimeout=receiveTimeout;
	}

	public int getReceiveTimeout() {
		return receiveTimeout;
	}

	public void setTransacted(boolean transacted) {
		this.transacted=transacted;
	}

	public boolean getTransacted() {
		return transacted;
	}

	public boolean getRedelivered() {
		return redelivered;
	}

    public synchronized void onException(JMSException ex) {
        System.err.println("JMS Exception occured.  Ignoring.");
    }
    
	public void onMessage(Message m) {  
		
		recvMessage=m;
		
		myListener.messageReceived();
		
	}

    private void sleepSecs(int secs) {
		try {
            Thread.sleep(secs*1000);
        }
        catch (InterruptedException ie)
        {
            System.err.println("Interrupted");
            System.exit(1);
        }
    }

	@Override
	public void onCommand(Object command) {
		//System.out.println("Transport Command "+command.getClass());
	}

	@Override
	public void onException(IOException exception) {
		System.out.println("Transport Exception "+exception);		
	}

	@Override
	public void transportInterupted() {
		System.out.println("Transport Interrupted");		
	}

	@Override
	public void transportResumed() {
		System.out.println("Transport Resumed");		
	}
} 
