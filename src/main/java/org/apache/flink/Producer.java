package org.apache.flink;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Productor de clicks de sitios Web. Lee de un fichero, extrae unicamente el nombre de la web y lo almacena en Kafka
 * Lee archivos locales debido a que 1usagov ha cerrado la URL en la que tenia disponibles los archivos en linea
 *
 * Clicks Producer. It reads from a file, gets only the name of the web and stores it into Kafka.
 * It reads local files because 1usagov website has finished the development and isn't offering data streaming, only historic files.
 *
 * Usage:
 *      "topic name (Kafka)" -> 1usagov
 *      "file path" -> (use file located in resources, for test purpose)
 *
 * @author miguel.infante.munoz
 * @updated daniel.coto.martin
 */
public class Producer {

    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
    public static final Pattern PATTERN_SITE = Pattern.compile(".+\"u\":[ ]\"ht(.+)\\\\/\\\\/(.+?)((\\\\|\").+)?$");

    public static final int MAX_RETRIES =  1000;
    public static final int MIN_DELAY   =   250;
    public static final int MAX_DELAY   = 60000;

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.println("Usage: " +
                    Producer.class.getName() + " " +
                    "'{topic-name}' " +
                    "'{url-streamming-data}' ");
            return;
        }

        System.err.println(DATE_FORMAT.format(new Date()) + "\t" +
                "INFO" + "\t" + "Starting " + KafkaProducer.class.getName() + "...");

        String topic = args[0];
        String source = args[1];

        System.err.println(DATE_FORMAT.format(new Date()) + "\t"  +
                "INFO"                         + "\t"  +
                "Listen to '"   + source       + "', " +
                "Writing to '"  + topic);

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        int retries = 0;
        int delay = MIN_DELAY;
        while (retries < MAX_RETRIES) {
            org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<String, String>(props);
            URLConnection uc;
            BufferedReader in = null;
            try {
                uc = new URL(source).openConnection();
                uc.connect();
                in = new BufferedReader(new InputStreamReader(uc.getInputStream()));
                String s = null;
                while ((s = in.readLine()) != null) {
                    Matcher m = PATTERN_SITE.matcher(s);
                    if (!m.find()) {
                        continue;}
                    System.out.println(m.group(2));
                    producer.send(new ProducerRecord<String, String>(topic, m.group(2)));
                    delay = MIN_DELAY;
                    retries = 1;
                }
                in.close();
            }
            catch(Exception e) {
                if (retries == 0) {
                    System.err.println(DATE_FORMAT.format(new Date()) + "\t" +
                            "ERROR" + "\t" +
                            "Generic Exception. " + e);
                    e.printStackTrace();
                }
                if (++retries > MAX_RETRIES) {
                    System.err.println(DATE_FORMAT.format(new Date()) + "\t" +
                            "FATAL" + "\t" +
                            "Maximum retries of requests exceeded. Program will exit. " + e);
                    e.printStackTrace();
                }
            }
            finally {
                producer.close();
                if (in != null) {
                    try {
                        in.close();
                    } catch(Exception ignore) {}
                }
            }
            System.err.println(DATE_FORMAT.format(new Date()) + "\t" +
                    "INFO" + "\t" +
                    "Retrying in " + delay +
                    " milliseconds...");
            try {
                Thread.sleep(delay);
            } catch (InterruptedException ignore) {}
            if (delay < MAX_DELAY) delay *= 2;
        }
    }
}