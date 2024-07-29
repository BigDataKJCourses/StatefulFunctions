package com.example.bigdata.model;

public class ScoreEvent {
    private String house;
    private String character;
    private int score;
    private long ts;

    // Konstruktor bezargumentowy
    public ScoreEvent() {
    }

    // Konstruktor atrybutowy
    public ScoreEvent(String house, String character, int score, long ts) {
        this.house = house;
        this.character = character;
        this.score = score;
        this.ts = ts;
    }

    // Gettery i settery
    public String getHouse() {
        return house;
    }

    public void setHouse(String house) {
        this.house = house;
    }

    public String getCharacter() {
        return character;
    }

    public void setCharacter(String character) {
        this.character = character;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "ScoreEvent{" +
                "house='" + house + '\'' +
                ", character='" + character + '\'' +
                ", score=" + score +
                ", ts=" + ts +
                '}';
    }
}


