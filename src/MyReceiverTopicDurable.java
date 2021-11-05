import javax.jms.*;  
import javax.naming.InitialContext;  
  
public class MyReceiverTopicDurable {  
    public static void main(String[] args) {  
        try {  
            //1) Create and start connection  
            InitialContext ctx=new InitialContext();  
            TopicConnectionFactory f=(TopicConnectionFactory)ctx.lookup("myTopicConnectionFactory");  
            TopicConnection con=f.createTopicConnection();
            con.setClientID("ClientID_"+(args.length>0?args[0]:"0"));
  
            con.start();  
            //2) create topic session  
            TopicSession ses=con.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);  
            //3) get the Topic object  
            Topic t=(Topic)ctx.lookup("myTopic");  
            //4)create TopicSubscriber  
            TopicSubscriber receiver=ses.createDurableSubscriber(t,"Test_Durable_Subscriber");  
              
            //5) create listener object  
            MyListener listener=new MyListener();  
              
            //6) register the listener object with subscriber  
            receiver.setMessageListener(listener);  
                          
            System.out.println("Subscriber1 is ready, waiting for messages...");  
            System.out.println("press Ctrl+c to shutdown..."); 
            while (true) {
            //while(!listener.text.contains("END")){                  
                Thread.sleep(1000);  
            }
            //con.close();
        }catch(Exception e){System.out.println(e);}  
    }  
  
}