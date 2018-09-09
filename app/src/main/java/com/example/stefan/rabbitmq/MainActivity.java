package com.example.stefan.rabbitmq;

import android.os.Handler;
import android.os.Message;
import android.os.StrictMode;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.util.Log;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeoutException;

public class MainActivity extends AppCompatActivity {

    private static final String QUEUE_NAME = "first_queue";
    private static final String TAG = "MainActivity";

    private ConnectionFactory factory = new ConnectionFactory();
    private Thread sendThread, recvThread;
    private Channel recvChannel;
    private Handler handler = new IncomingHandler(this);
    private BlockingDeque<String> localQueue = new LinkedBlockingDeque<>();
    private boolean sendingActive = true;

    private TextView textView;
    private EditText editText;


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        initViews();
        basicSetup();
        startPublisher();
        startSubscriber();
    }

    @Override
    protected void onDestroy() {
        super.onDestroy();
        sendingActive = false;
        endSubscriber();
    }

    private void initViews() {
        textView = findViewById(R.id.text_message);
        editText = findViewById(R.id.edit_message);
        findViewById(R.id.btn_send_message).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                sendMessage();
            }
        });
    }

    private void basicSetup() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                factory.setHost("10.0.2.2");
//                    factory.setHost("10.14.116.218");
//                    factory.setPort(5672);
//                    factory.setVirtualHost("/");
//                    factory.setUsername("guest");
//                    factory.setPassword("guest");
//                    StrictMode.ThreadPolicy policy = new StrictMode.ThreadPolicy.Builder().permitAll().build();
//                    StrictMode.setThreadPolicy(policy);
            }
        }).start();
    }


    private void startPublisher() {
        sendThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (sendingActive) {
                    try {
                        Connection connection = factory.newConnection();
                        Channel channel = connection.createChannel();
                        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

                        while (sendingActive) {
                            String message = localQueue.takeFirst();
                            try {
                                channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
                                Log.i(TAG, "PUBLISHED");
//                                channel.waitForConfirmsOrDie();
                            } catch (Exception e) {
                                e.printStackTrace();
                                Log.e(TAG, "ERROR AFTER PUBLISH");
                                localQueue.putFirst(message);
                                throw e;
                            }
                        }

                        channel.close();
                        connection.close();

                    } catch (InterruptedException e) {
                        Log.i(TAG, "INTERRUPTED! APPLICATION CLOSED");
                        e.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                        Log.d(TAG, "Connection broken! ");
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException e1) {
                            break;
                        }
                    }
                }
            }
        });
        sendThread.start();
    }

    public void startSubscriber() {
        recvThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    recvChannel = factory.newConnection().createChannel();
                    Consumer consumer = new DefaultConsumer(recvChannel) {
                        @Override
                        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                            String message = new String(body, "UTF-8");
                            Message msg = handler.obtainMessage();
                            Bundle bundle = new Bundle();
                            bundle.putString("msg", message);
                            msg.setData(bundle);
                            handler.sendMessage(msg);
                            Log.i(TAG, "handleDelivery: MESSAGE CONSUMED");
                        }
                    };
                    recvChannel.basicConsume(QUEUE_NAME, true, consumer);
                } catch (IOException | TimeoutException e) {
                    e.printStackTrace();
                }
            }
        });
        recvThread.start();

    }

    public void endSubscriber() {
        try {
            recvChannel.close();
            recvThread.interrupt();
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }

    public void sendMessage() {
        try {
            String message = editText.getText().toString();
            localQueue.putLast(message);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    static class IncomingHandler extends Handler {
        private final WeakReference<MainActivity> activityWeakReference;

        IncomingHandler(MainActivity activity) {
            activityWeakReference = new WeakReference<MainActivity>(activity);
        }

        @Override
        public void handleMessage(Message msg) {
            TextView textView1 = activityWeakReference.get().findViewById(R.id.text_message);
            textView1.append(msg.getData().getString("msg") + "\n");

        }
    }
}
