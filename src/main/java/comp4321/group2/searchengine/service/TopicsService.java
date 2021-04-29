package comp4321.group2.searchengine.service;

import comp4321.group2.searchengine.apimodels.Topic;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Service
public class TopicsService {

    private final List<Topic> topicList = new ArrayList<>(Arrays.asList(

        new Topic("_spring", "_Spring FrameWork", "_Spring Description"),
        new Topic("spring", "Spring FrameWork", "Spring Description"),
        new Topic("java", "Java FrameWork", "Java Description")

    ));

    public List<Topic> getAllTopicList() {
        return topicList;
    }

    public Topic getTopic(String id) {
        return topicList.stream().filter(topic -> topic.getId().equals(id)).findFirst().get();
    }

    public void addTopic(Topic topic) {
        topicList.add(topic);
    }

    public void updateTopic(Topic topic, String id) {
        int counter = 0;
        for (Topic topic1 : topicList) {
            if (topic1.getId().equals(id)) {
                topicList.set(counter, topic);
            }
            counter++;
        }
    }

    public void deleteTopic(String id) {
        topicList.removeIf(topic -> topic.getId().equals(id));
    }
}
