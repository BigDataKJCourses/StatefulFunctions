package com.example.bigdata.testdata;

public class Inputs {
    private static final String[] jsonStringsOrdered = {
            "{\"house\":\"Gryffindor\",\"character\":\"Harry Potter\",\"score\":\"100\",\"ts\":\"2023-08-10 12:34:56.000\"}",
            "{\"house\":\"Gryffindor\",\"character\":\"Sirius Black\",\"score\":\"70\",\"ts\":\"2023-08-10 12:35:00.000\"}",
            "{\"house\":\"Ravenclaw\",\"character\":\"Padma Patil\",\"score\":\"85\",\"ts\":\"2023-08-10 13:25:00.000\"}",
            "{\"house\":\"Slytherin\",\"character\":\"Draco Malfoy\",\"score\":\"75\",\"ts\":\"2023-08-10 13:45:23.000\"}",
            "{\"house\":\"Ravenclaw\",\"character\":\"Luna Lovegood\",\"score\":\"90\",\"ts\":\"2023-08-10 14:56:34.000\"}",
            "{\"house\":\"Gryffindor\",\"character\":\"Hermione Granger\",\"score\":\"95\",\"ts\":\"2023-08-10 15:10:00.000\"}",
            "{\"house\":\"Slytherin\",\"character\":\"Hermione Granger\",\"score\":\"85\",\"ts\":\"2023-08-10 15:30:00.000\"}",
            "{\"house\":\"Ravenclaw\",\"character\":\"Cho Chang\",\"score\":\"80\",\"ts\":\"2023-08-10 15:55:00.000\"}",
            "{\"house\":\"Gryffindor\",\"character\":\"Ron Weasley\",\"score\":\"85\",\"ts\":\"2023-08-10 16:20:00.000\"}",
            "{\"house\":\"Slytherin\",\"character\":\"Harry Potter\",\"score\":\"60\",\"ts\":\"2023-08-10 16:45:00.000\"}",
            "{\"house\":\"Ravenclaw\",\"character\":\"Hermione Granger\",\"score\":\"95\",\"ts\":\"2023-08-10 17:10:00.000\"}",
            "{\"house\":\"Slytherin\",\"character\":\"Draco Malfoy\",\"score\":\"80\",\"ts\":\"2023-08-10 18:00:00.000\"}",
            "{\"house\":\"Gryffindor\",\"character\":\"Draco Malfoy\",\"score\":\"60\",\"ts\":\"2023-08-10 18:50:00.000\"}",
            "{\"house\":\"Slytherin\",\"character\":\"Hermione Granger\",\"score\":\"70\",\"ts\":\"2023-08-10 19:15:00.000\"}",
            "{\"house\":\"Ravenclaw\",\"character\":\"Harry Potter\",\"score\":\"90\",\"ts\":\"2023-08-10 19:40:00.000\"}",
            "{\"house\":\"Gryffindor\",\"character\":\"Harry Potter\",\"score\":\"80\",\"ts\":\"2023-08-10 20:05:00.000\"}",
            "{\"house\":\"Slytherin\",\"character\":\"Hermione Granger\",\"score\":\"50\",\"ts\":\"2023-08-10 20:30:00.000\"}",
            "{\"house\":\"Ravenclaw\",\"character\":\"Draco Malfoy\",\"score\":\"75\",\"ts\":\"2023-08-10 20:55:00.000\"}",
            "{\"house\":\"Gryffindor\",\"character\":\"George Weasley\",\"score\":\"90\",\"ts\":\"2023-08-10 21:20:00.000\"}",
            "{\"house\":\"Slytherin\",\"character\":\"Harry Potter\",\"score\":\"70\",\"ts\":\"2023-08-10 21:45:00.000\"}",
            "{\"house\":\"Ravenclaw\",\"character\":\"Harry Potter\",\"score\":\"85\",\"ts\":\"2023-08-10 22:10:00.000\"}"
    };

    private static final String[] jsonStringsUnordered = {
            "{\"house\":\"Gryffindor\",\"character\":\"Harry Potter\",\"score\":\"100\",\"ts\":\"2023-08-10 12:34:56.000\"}",
            "{\"house\":\"Slytherin\",\"character\":\"Draco Malfoy\",\"score\":\"75\",\"ts\":\"2023-08-10 13:45:23.000\"}",
            "{\"house\":\"Ravenclaw\",\"character\":\"Luna Lovegood\",\"score\":\"90\",\"ts\":\"2023-08-10 14:56:34.000\"}",
            "{\"house\":\"Slytherin\",\"character\":\"Hermione Granger\",\"score\":\"85\",\"ts\":\"2023-08-10 15:30:00.000\"}",
            "{\"house\":\"Ravenclaw\",\"character\":\"Cho Chang\",\"score\":\"80\",\"ts\":\"2023-08-10 15:55:00.000\"}",
            "{\"house\":\"Slytherin\",\"character\":\"Harry Potter\",\"score\":\"60\",\"ts\":\"2023-08-10 16:45:00.000\"}",
            "{\"house\":\"Gryffindor\",\"character\":\"Hermione Granger\",\"score\":\"95\",\"ts\":\"2023-08-10 15:10:00.000\"}",
            "{\"house\":\"Gryffindor\",\"character\":\"Sirius Black\",\"score\":\"70\",\"ts\":\"2023-08-10 12:35:00.000\"}",
            "{\"house\":\"Ravenclaw\",\"character\":\"Padma Patil\",\"score\":\"85\",\"ts\":\"2023-08-10 13:25:00.000\"}",
            "{\"house\":\"Slytherin\",\"character\":\"Draco Malfoy\",\"score\":\"80\",\"ts\":\"2023-08-10 18:00:00.000\"}",
            "{\"house\":\"Gryffindor\",\"character\":\"Draco Malfoy\",\"score\":\"60\",\"ts\":\"2023-08-10 18:50:00.1\"}",
            "{\"house\":\"Slytherin\",\"character\":\"Hermione Granger\",\"score\":\"70\",\"ts\":\"2023-08-10 19:15:00.20\"}",
            "{\"house\":\"Ravenclaw\",\"character\":\"Harry Potter\",\"score\":\"90\",\"ts\":\"2023-08-10 19:40:00.300\"}",
            "{\"house\":\"Ravenclaw\",\"character\":\"Hermione Granger\",\"score\":\"95\",\"ts\":\"2023-08-10 17:10:00.000\"}",
            "{\"house\":\"Gryffindor\",\"character\":\"Harry Potter\",\"score\":\"80\",\"ts\":\"2023-08-10 20:05:00.000\"}",
            "{\"house\":\"Slytherin\",\"character\":\"Hermione Granger\",\"score\":\"50\",\"ts\":\"2023-08-10 20:30:00.000\"}",
            "{\"house\":\"Ravenclaw\",\"character\":\"Draco Malfoy\",\"score\":\"75\",\"ts\":\"2023-08-10 20:55:00.000\"}",
            "{\"house\":\"Gryffindor\",\"character\":\"George Weasley\",\"score\":\"90\",\"ts\":\"2023-08-10 21:20:00.000\"}",
            "{\"house\":\"Gryffindor\",\"character\":\"Ron Weasley\",\"score\":\"85\",\"ts\":\"2023-08-10 16:20:00.000\"}",
            "{\"house\":\"Slytherin\",\"character\":\"Harry Potter\",\"score\":\"70\",\"ts\":\"2023-08-10 21:45:00.000\"}",
            "{\"house\":\"Ravenclaw\",\"character\":\"Harry Potter\",\"score\":\"85\",\"ts\":\"2023-08-10 22:10:00.000\"}"
    };

    public static String[] getJsonOrderedStrings() {
        return jsonStringsOrdered;
    }

    public static String[] getJsonUnorderedStrings() {
        return jsonStringsUnordered;
    }
}

