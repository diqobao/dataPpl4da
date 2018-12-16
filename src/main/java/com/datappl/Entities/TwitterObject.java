package com.datappl.Entities;

import java.io.Serializable;
import java.util.List;

public class TwitterObject implements Serializable {

    private long eventId;
    private long UserId;
    private boolean hasLocation;
    private int country;
    private String city;
    private List<String> hashTags;
    private String text;
    private int textLength;

    public TwitterObject() {

    }

//    TODO: complete implementation
    @Override
    public String toString() {
        return "";
    }

    public long getEventId() {
        return eventId;
    }

    public long getUserId() {
        return UserId;
    }

    public boolean isHasLocation() {
        return hasLocation;
    }

    public int getCountry() {
        return country;
    }

    public int getTextLength() {
        return textLength;
    }

    public List<String> getHashTags() {
        return hashTags;
    }

    public String getCity() {
        return city;
    }

    public String getText() {
        return text;
    }

}
