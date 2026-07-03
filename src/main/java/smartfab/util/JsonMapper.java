package smartfab.util;

import tools.jackson.databind.ObjectMapper;

/**
 * @author Gianluca Bianchi
 * 
 *      JsonMapper utity class for managin JSON serialization 
 *      and deserialization in a centralized way
 */
public class JsonMapper {
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * 
     * @param <T>
     * @param json
     * @param clazz
     * @return Deserialized Object
     */
    public static <T> T deserialize(String json, Class<T> clazz) {
        try {
            return mapper.readValue(json, clazz);
        } catch (Exception e) {
            throw new RuntimeException("Errore di parsing JSON", e);
        }
    }

    /**
     * 
     * @param <T>
     * @param object
     * @return JSON stirng of serialized object
     */
    public static <T> String serialize(T object){
        return mapper.writeValueAsString(object);
    }
}