package com.confluex.mule.example;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.sql.DataSource;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mule.api.MuleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jms.JmsException;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessagePostProcessor;
import org.springframework.jms.listener.SimpleMessageListenerContainer;

import com.confluex.mule.test.BeforeMule;
import com.confluex.mule.test.BetterFunctionalTestCase;
import com.confluex.test.jms.CountingMessageListener;

public class XaJdbcJmsLoadTest extends BetterFunctionalTestCase {
    private static final int QUEUE_SIZE = 20000;
    Logger log = LoggerFactory.getLogger(getClass());
    
    private ConnectionFactory amqConnectionFactory;

    private SimpleMessageListenerContainer listenerContainer;
    private CountingMessageListener listener;
    
    private JdbcTemplate jdbc;
    private JmsTemplate jms;
        
    @BeforeClass
    public static void initTestTimeout() {
        System.setProperty("mule.test.timeoutSecs", "720");
    }
    
    @Override
    public String getConfigFile() {
        return "example-xa-under-load.xml";
    }
    
    @BeforeMule
    public void fillInputQueueWithManyMessages(MuleContext muleContext) throws Exception {
        String exampleInput1 = IOUtils.toString(new ClassPathResource("example-input-1.txt").getInputStream());
        String exampleInput2 = IOUtils.toString(new ClassPathResource("example-input-2.txt").getInputStream());
        
        ActiveMQConnectionFactory activeMqConnectionFactory = muleContext.getRegistry().lookupObject("amqConnectionFactory");
        log.debug("Using JMS broker at " + activeMqConnectionFactory.getBrokerURL());
        amqConnectionFactory = new CachingConnectionFactory(activeMqConnectionFactory);
        jms = new JmsTemplate(amqConnectionFactory);
        jms.setReceiveTimeout(5000);
        

        for (int i = 0; i < QUEUE_SIZE; i++) {
            if (i % 2 == 0) {
                sendToJms("input", exampleInput1);
            } else {
                sendToJms("input", exampleInput2);
            }
            if (i % 100 == 0) {
                log.debug(String.format("Queued %d messages", i));
            }
        }
        log.info(String.format("Queued %d messages", QUEUE_SIZE));
    }
    
    @Before
    public void initListeners() {
        listener = new CountingMessageListener();
        
        listenerContainer = new SimpleMessageListenerContainer();
        listenerContainer.setMessageListener(listener);
        listenerContainer.setDestinationName("output");
        listenerContainer.setConnectionFactory(amqConnectionFactory);
        listenerContainer.start();
    }
    
    @Before
    public void initJdbc() {
        DataSource dataSource = muleContext.getRegistry().lookupObject("esbXaDataSource");
        jdbc = new JdbcTemplate(dataSource);
    }
    
    @After
    public void stopListeners() {
        listenerContainer.stop();
    }
    
    @Test
    public void messageRateShouldStaySomewhatConsistentUnderLoad() throws InterruptedException {
        long start = System.currentTimeMillis();
        final int MAX_CONNECTIONS = 16;
        final int MAX_TOTAL_TIME = 600000;
        int maxConnections = 0;
        
        // wait for the first message, to account for startup time 
        while (listener.getCount() < 1 && System.currentTimeMillis() < start + MAX_TOTAL_TIME) {
            Thread.sleep(100);
        }
        
        // Get baseline timing
        long longestBatchTime = 0;
        for (int i = 0; i < 3; i++) {
            long batchStart = System.currentTimeMillis();
            while (listener.getCount() < 100 * (i + 1) && System.currentTimeMillis() < start + MAX_TOTAL_TIME) {
                Thread.sleep(200);
            }
            long thisBatch = System.currentTimeMillis() - batchStart;
            maxConnections = getMaxConnections(maxConnections);
            log.debug(String.format("Batch %d took %d ms", i, thisBatch));
            longestBatchTime = Math.max(longestBatchTime, thisBatch);
        }
        int counted = 300;
        log.info(String.format("Baseline batch time: %d ms for 100 messages", longestBatchTime));
        while (listener.getCount() < QUEUE_SIZE && System.currentTimeMillis() < start + MAX_TOTAL_TIME) {
            long batchStart = System.currentTimeMillis();
            while (listener.getCount() < counted + 100 && System.currentTimeMillis() < start + MAX_TOTAL_TIME) {
                Thread.sleep(200);
            }
            long thisBatch = System.currentTimeMillis() - batchStart;
            log.info(String.format("Batch of 100 completed in %d ms", thisBatch));
            assertTrue("Batch took 10 times longer than the baseline", thisBatch < longestBatchTime * 10);
            maxConnections = getMaxConnections(maxConnections);
            counted += 100;
        }

        assertTrue("Too many open connections: " + maxConnections, maxConnections <= MAX_CONNECTIONS);
        assertTrue("Took too long to complete", System.currentTimeMillis() < start + MAX_TOTAL_TIME);
    }
    
    private int getMaxConnections(int currentMax) {
        int connections = jdbc.queryForInt("select count(*) from information_schema.sessions;");
        log.debug(String.format("Messages: %d, Connections %d", listener.getCount(), connections));        
        return Math.max(connections, currentMax);
    }
    
    protected void sendToJms(String destination, Object payload) throws JmsException {
        sendToJms(destination, payload, new HashMap<String, String>());
    }

    protected void sendToJms(String destination, Object payload, final Map<String, String> headers) {
        jms.convertAndSend(destination, payload, new MessagePostProcessor() {
            @Override
            public Message postProcessMessage(Message message) throws JMSException {
                for (String headerKey : headers.keySet()) {
                    message.setStringProperty(headerKey, headers.get(headerKey));
                }
                return message;
            }
        });
    }
    
    protected String receiveFromJms(String destination) {
        return (String) jms.receiveAndConvert(destination);
    }
}
