package com.example.bigdata.tools;

import org.apache.flink.api.java.utils.ParameterTool;

public class Properties {
    public static ParameterTool get(String[] args) {

        ParameterTool propertiesFromArgs = ParameterTool.fromArgs(args);

        if (propertiesFromArgs.has("params")) {
            String paramsFilePath = propertiesFromArgs.get("params");
            try {
                // Ładowanie parametrów z pliku
                ParameterTool propertiesFromFile = ParameterTool.fromPropertiesFile(paramsFilePath);
                // Łączenie parametrów z pliku z parametrami z linii poleceń
                return propertiesFromFile.mergeWith(propertiesFromArgs);
            } catch (Exception e) {
                throw new RuntimeException("Nie udało się załadować pliku z parametrami: " + paramsFilePath, e);
            }
        } else {
            return propertiesFromArgs;
        }
    }
}