package org.streaming;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class KafkaTwitterProducer {

    public static void main(String[] args) throws Exception {

        final LinkedBlockingQueue<Status> queue = new LinkedBlockingQueue<Status>(1000);

        if (args.length < 3) {
            System.out.println("Usage: KafkaTwitterProducer <token-file-path> <topic-name> <twitter-search-keywords>");
            return;
        }
        List<String> tokens = getAuthTokens(args[0]);
        String consumerKey = tokens.get(0);
        String consumerSecret = tokens.get(1);
        String accessToken = tokens.get(2);
        String accessTokenSecret = tokens.get(3);

        String topicName = args[1];
        String[] searchWords = args[2].split(",");

        // Set twitter oAuth tokens in the configuration
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey(consumerKey)
                .setOAuthConsumerSecret(consumerSecret)
                .setOAuthAccessToken(accessToken)
                .setOAuthAccessTokenSecret(accessTokenSecret);

        // Create twitter stream using the configuration
        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        StatusListener listener = new StatusListener() {
            public void onStatus(Status status) {
                queue.offer(status);
            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }

            public void onTrackLimitationNotice(int i) {
                System.out.println("Got track limitation notice:" + i);
            }

            public void onScrubGeo(long userId, long upToStatusId) {
                System.out.println("Got scrub_geo event userId:" + userId + "upToStatusId:" + upToStatusId);
            }

            public void onStallWarning(StallWarning stallWarning) {
                System.out.println("Got stall warning:" + stallWarning);
            }

            public void onException(Exception e) {
                e.printStackTrace();
            }
        };
        twitterStream.addListener(listener);
        FilterQuery query = new FilterQuery(searchWords);
        query.language("en");
        twitterStream.filter(query);

        configKafkaProducer(queue, topicName);
    }
    private static void configKafkaProducer(LinkedBlockingQueue<Status> q, String topic) throws Exception {

        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        int j = 0;
        String msg = null;
        while (true) {
            Status ret = q.poll();
            if (ret == null) {
                Thread.sleep(100);
            }
            else {
                String location = "";
                if (ret.getUser().getLocation() != null && ret.getHashtagEntities().length > 0) {
                    String tweet = ret.getText();
                    location = ret.getUser().getLocation();
                    System.out.println("Tweet:" + tweet);
                    System.out.println("Location: " + location);
                    // Use /TLOC/ as a separator pattern
                    msg = location + " /TLOC/ " + tweet;
                }
                producer.send(new ProducerRecord<String, String>(topic, Integer.toString(j++), msg));
            }
            Thread.sleep(100);
        }
    }

    private static List<String> getAuthTokens(String input) {
        final List<String> tokens = new ArrayList<String>();
        try {
            File f = new File(input);
            BufferedReader rdr = new BufferedReader(new FileReader(f));
            String readLine;
            while ((readLine = rdr.readLine()) != null) {
                tokens.add(readLine);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return tokens;
    }
}
