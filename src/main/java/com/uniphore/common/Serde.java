package com.uniphore.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

public class Serde {

    public static byte[] serialize(Message message) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                ObjectOutputStream writer = new ObjectOutputStream(out)) {
            writer.writeObject(message);
            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Message deserialize(byte[] bytes) {
        try (ByteArrayInputStream in = new ByteArrayInputStream(bytes);
                ObjectInputStream reader = new ObjectInputStream(in)) {
            return (Message) reader.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

}
