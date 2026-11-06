package com.blue.flink.rt.bean;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.Timestamp;

public class Event {
    public String id;
    public String url;
    public Long ts;
    public String dt;

    public static ObjectMapper objectMapper = new ObjectMapper();

    public static String toJsonString(Event e) throws Exception {
        return objectMapper.writeValueAsString(e);
    }

    public static Event fromJsonString(String json) throws Exception {
        return objectMapper.readValue(json, Event.class);
    }

    public Event() {

    }

    public Event(String id, String url, long ts) {

        this.id = id;
        this.url = url;
        this.ts = ts;
        this.dt = new Timestamp(ts).toString();
    }

    public Event(String id, String url, Long ts, String dt) {
        this.id = id;
        this.url = url;
        this.ts = ts;
        this.dt = dt;
    }

    @Override
    public String toString() {
        return "Event{" +
                "id='" + id + '\'' +
                ", url='" + url + '\'' +
                ", ts=" + ts +
                ", dt='" + dt + '\'' +
                '}';
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public void setDt(String dt) {
        this.dt = dt;
    }

    public String getId() {
        return id;
    }

    public String getUrl() {
        return url;
    }

    public Long getTs() {
        return ts;
    }

    public String getDt() {
        return dt;
    }
}