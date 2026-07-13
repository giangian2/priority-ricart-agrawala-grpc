package smartfab.http.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import smartfab.model.mqtt.MqttClientManager;

@Configuration
public class MqttConfig {
    
    @Bean
    public MqttClientManager mqtt(){
        return MqttClientManager.getInstance();
    }
}
