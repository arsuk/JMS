import java.util.Arrays;

public class Main {
	
	public static void main (String[] args) {

        if (args.length==0) {
    		helpText();
        } else {
            String[] nargs=Arrays.copyOfRange(args, 1, args.length);
            switch(args[0]) {
              case "MyReceiver":
                MyReceiver.main(nargs);
                break;
              case "MySender":
                MySender.main(nargs);
                break;
              case "MySenderPoolded":
                MySenderPooled.main(nargs);
                break;
              case "MyReceiverTopic":
                MyReceiverTopic.main(nargs);
                break;
              case "MySenderTopic":
                MySenderTopic.main(nargs);
                break;
              case "MySenderTopicDurable":
                MyReceiverTopicDurable.main(nargs);
                break;
              default:
                helpText();
            }
        }      
	}

    static void helpText() {
        System.out.println("The JMS tools are:");
        System.out.println("MyReceiver, MySender, MySenderPooled, ConnectionTest, MyReceiverTopic, MySenderTopic, MyReceiverTopicDurable");
        System.out.println("They can be run using:");
        System.out.println("    java -cp target/jms-example-1.0.jar <tool-name> <args>");
        System.out.println("or: java -jar target/jms-example-1.0.jar <tool-name> <args>");
    }
}
