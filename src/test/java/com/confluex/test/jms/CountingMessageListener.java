package com.confluex.test.jms;

import javax.jms.Message;
import javax.jms.MessageListener;

public class CountingMessageListener implements MessageListener {
    private int count = 0;

    @Override
    public void onMessage(Message message) {
        count++;
    }
    
    public int getCount() {
        return count;
    }
    
}
