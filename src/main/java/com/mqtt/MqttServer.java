package com.mqtt;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.HashMap;
import java.util.Map;

public class MqttServer {
    @Value("${mqtt.host}")
    private String mqtt_host;

    @Value("${mqtt.userName}")
    private String userName;

    @Value("${mqtt.password}")
    private String password;

    private MqttClient client;
    private MqttTopic topic;

    public static Map<String, MqttClient> clientMap = new HashMap<String, MqttClient>();

    @RequestMapping(value = "/publishMessage")
    @ResponseBody
    public Boolean publishTopic(String clientid,String messageContent,String topicContent) throws MqttException {

        if(StringUtils.isNotBlank(clientid) && StringUtils.isNotBlank(messageContent) && StringUtils.isNotBlank(topicContent)){
            // host为主机名，clientid即连接MQTT的客户端ID，一般以唯一标识符表示，MemoryPersistence设置clientid的保存形式，默认为以内存保存
            //client = new MqttClient(HOST, clientid, new MemoryPersistence());
            client = new MqttClient(mqtt_host, clientid, new MemoryPersistence());
            // MQTT的连接设置
            MqttConnectOptions options = getOptions();
            // 设置回调
            client.setCallback(new PushCallback());

            client.connect(options);
            clientMap.put(clientid, client);
            topic = client.getTopic(topicContent);

            MqttMessage message = new MqttMessage();
            // 设置服务质量
            message.setQos(2);
            // 设置是否在服务器中保存消息体
            message.setRetained(true);
            // 设置消息的内容
            message.setPayload(messageContent.getBytes());
            // 发布消息
            publish(topic , message);
            return Boolean.TRUE;
        }else{
            return Boolean.FALSE;
        }
    }

    @RequestMapping(value = "/subscribeTopic")
    @ResponseBody
    public Boolean subscribeTopic(String clientid,String topicContent) throws MqttException {

        if(StringUtils.isNotBlank(clientid) && StringUtils.isNotBlank(topicContent)){
            client = clientMap.get(clientid);
            if(null == client) {
                // host为主机名，clientid即连接MQTT的客户端ID，一般以唯一标识符表示，MemoryPersistence设置clientid的保存形式，默认为以内存保存
                //client = new MqttClient(HOST, clientid, new MemoryPersistence());
                client = new MqttClient(mqtt_host, clientid, new MemoryPersistence());
                // MQTT的连接设置
                MqttConnectOptions options = getOptions();
                // 设置回调
                client.setCallback(new PushCallback());

                client.connect(options);
                clientMap.put(clientid, client);
            }else {
                if (!client.isConnected()) {
                    // MQTT的连接设置
                    MqttConnectOptions options = getOptions();
                    // 设置回调
                    client.setCallback(new PushCallback());

                    client.connect(options);
                    System.out.println("连接成功");
                    clientMap.put(clientid, client);
                }
            }
            topic = client.getTopic(topicContent);

            //订阅消息
            int[] Qos = {2};
            String[] topic1 = {topicContent};
            client.subscribe(topic1, Qos);
            return Boolean.TRUE;
        }else{
            return Boolean.FALSE;
        }
    }

    public MqttConnectOptions getOptions() {
        // MQTT的连接设置
        MqttConnectOptions options = new MqttConnectOptions();
        // 设置是否清空session,这里如果设置为false表示服务器会保留客户端的连接记录，这里设置为true表示每次连接到服务器都以新的身份连接
        // 换而言之，设置为false时可以客户端可以接受离线消息
        options.setCleanSession(false);
        // 设置连接的用户名
        options.setUserName(userName);
        // 设置连接的密码
        options.setPassword(password.toCharArray());
        // 设置超时时间 单位为秒
        options.setConnectionTimeout(10);
        // 设置会话心跳时间 单位为秒 服务器会每隔1.5*20秒的时间向客户端发送个消息判断客户端是否在线，但这个方法并没有重连的机制
        options.setKeepAliveInterval(20);
        return options;
    }

    public void publish(MqttTopic topic , MqttMessage message)
            throws MqttPersistenceException,MqttException {
        MqttDeliveryToken token = topic.publish(message);
        token.waitForCompletion();
        System.out.println("message is published completely! " + token.isComplete());
    }
}
