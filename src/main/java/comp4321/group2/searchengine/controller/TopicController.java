package comp4321.group2.searchengine.controller;

import comp4321.group2.searchengine.apimodels.Topic;
import comp4321.group2.searchengine.service.TopicsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
public class TopicController {

    @Autowired
    private TopicsService topicsService;

    @RequestMapping("/topics")
    public List<Topic> all_topics() {
        return topicsService.getAllTopicList();
    }

    @RequestMapping("/topics/{id}")
    public Topic getTopic(@PathVariable("id") String id) {
        return topicsService.getTopic(id);
    }

    @RequestMapping(method = RequestMethod.POST, value = "/topics")
    public void addTopic(@RequestBody Topic topic) {
        topicsService.addTopic(topic);
    }

    @RequestMapping(method = RequestMethod.PUT, value = "/topics/{id}")
    public void updateTopic(@RequestBody Topic topic, @PathVariable("id") String id) {
        topicsService.updateTopic(topic, id);
    }

    @RequestMapping(method = RequestMethod.DELETE, value = "/topics/{id}")
    public void deleteTopic(@PathVariable("id") String id) {
        topicsService.deleteTopic(id);
    }
}
