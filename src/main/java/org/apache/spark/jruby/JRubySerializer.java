package org.apache.spark.jruby;

import org.apache.spark.SparkConf;
import org.apache.spark.serializer.*;
import scala.reflect.ClassTag;
import org.apache.spark.util.ByteBufferInputStream;

import java.io.*;
import java.nio.ByteBuffer;


/**
 * Created by chenyh on 3/24/16.
 */

class JRubySerializationStream extends SerializationStream {
    private ObjectOutputStream objOut;
    private int counter = 0;
    private int counterReset;
    private boolean extraDebugInfo;

    static class JRubyObjectOutputStream extends ObjectOutputStream {

        public JRubyObjectOutputStream(OutputStream outputStream) throws IOException {
            super(outputStream);
        }
    }

    @Override
    public <T> SerializationStream writeObject(T t, ClassTag<T> evidence$4) {
        System.err.println("BBBB " + t.getClass().getName());
        try {
            objOut.writeObject(t);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        counter += 1;
        if (counterReset > 0 && counter >= counterReset) {
            try {
                objOut.reset();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            counter = 0;
        }
        return this;
    }

    @Override
    public void flush() {
        try {
            objOut.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            objOut.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public JRubySerializationStream(OutputStream out, int counterReset, boolean extraDebugInfo) {
        this.counterReset = counterReset;
        this.extraDebugInfo = extraDebugInfo;
        try {
            objOut = new ObjectOutputStream(out);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

class JRubyDeserializationStream extends  DeserializationStream {
    private ObjectInputStream in;
    private ClassLoader loader;


    @Override
    public <T> T readObject(ClassTag<T> evidence$8) {
        try {
            return (T)in.readObject();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            in.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public JRubyDeserializationStream(InputStream in, ClassLoader loader) {
        this.loader = loader;
        try {
            this.in = new ObjectInputStream(in) {
                @Override
                protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
                    System.err.println("RESC: " + desc.getName());

                    return super.resolveClass(desc);
                }
            };
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

class JRubySerializerInstance extends SerializerInstance {
    private int counterReset;
    private boolean extraDebugInfo;
    private ClassLoader defaultClassLoader;

    public JRubySerializerInstance(int counterReset, boolean extraDebugInfo, ClassLoader cl) {
        this.counterReset = counterReset;
        this.extraDebugInfo = extraDebugInfo;
        this.defaultClassLoader = cl;
    }

    @Override
    public <T> ByteBuffer serialize(T t, ClassTag<T> evidence$1) {
        System.err.println("XXXXXXXXXX " + t.getClass().getName() + ", " + t.getClass().isPrimitive());
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        SerializationStream out = serializeStream(bos);
        out.writeObject(t, evidence$1);
        out.close();
        return ByteBuffer.wrap(bos.toByteArray());
    }

    @Override
    public <T> T deserialize(ByteBuffer bytes, ClassTag<T> evidence$2) {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes.array());
        DeserializationStream in = deserializeStream(bis);
        return in.readObject(evidence$2);
    }

    @Override
    public <T> T deserialize(ByteBuffer bytes, ClassLoader loader, ClassTag<T> evidence$3) {
        System.err.println("DES: " + loader.getClass().getName());
        ByteBufferInputStream bis = new ByteBufferInputStream(bytes, false);
        DeserializationStream in = new JRubyDeserializationStream(bis, loader);
        return in.readObject(evidence$3);
    }

    @Override
    public SerializationStream serializeStream(OutputStream s) {
        return new JRubySerializationStream(s, counterReset, extraDebugInfo);
    }

    @Override
    public DeserializationStream deserializeStream(InputStream s) {
        return new JRubyDeserializationStream(s, defaultClassLoader);
    }
}

public class JRubySerializer extends Serializer implements Externalizable {
    private SparkConf conf;
    private int counterReset;
    private boolean extraDebugInfo;

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {
        objectOutput.writeInt(counterReset);
        objectOutput.writeBoolean(extraDebugInfo);
    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        counterReset = objectInput.readInt();
        extraDebugInfo = objectInput.readBoolean();
    }

    @Override
    public SerializerInstance newInstance() {
        ClassLoader cl;
        if (defaultClassLoader().isEmpty()) {
            cl = Thread.currentThread().getContextClassLoader();
        } else {
            cl = defaultClassLoader().get();
        }
        return new JRubySerializerInstance(counterReset, extraDebugInfo, cl);
    }

    public JRubySerializer(SparkConf conf) {
        this.conf = conf;
        this.counterReset = conf.getInt("spark.serializer.objectStreamReset", 100);
        this.extraDebugInfo = conf.getBoolean("spark.serializer.extraDebugInfo", true);
    }
}
