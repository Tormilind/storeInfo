package MainPackage;


import InputReading.ParquetInputReader;
import InputReading.ParquetInputRecord;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import MainPackage.Venue;
import org.apache.avro.JsonProperties;

import static MainPackage.MapUtil.sortByValue;

@Slf4j
public class MainClass {

    private static int files = 0;
    private static Map<String, Map<LocalDate, Map<Integer, List<Long>>>> uniqueData = new LinkedHashMap<>();  //Äärmiselt keeruline map. Konspekt: Venue->Kuupäev->Tund->List<Token>
    private static Map<String, Map<LocalDate, Map<Integer, List<Long>>>> adIdData = new LinkedHashMap<>();
    private static Map<String, Integer> uniqueNumbers = new LinkedHashMap<>();
    private static Map<String, Map<LocalDate, List<Long>>> dayData = new LinkedHashMap<>();
    private static Map<String, Map<LocalDate, List<Long>>> adIdDayData = new LinkedHashMap<>();
    private static Map<String, Map<LocalDate, List<Float>>> dwellStats = new LinkedHashMap<>();
    private static Map<String, Map<LocalDate, Integer>> recordCount = new HashMap<>();
    private static StringBuilder statsText = new StringBuilder();
    private static Map<String, List<Venue>> venues = new HashMap<>();
    private static Map<String, Map<LocalDate, List<Float>>> bellDwellStats = new LinkedHashMap<>();
    private static int added = 0;


    public static void makeVenues() {
        File dataFolder = new File("./Data");
        File[] dataFiles = dataFolder.listFiles();
        ParquetInputReader parser = new ParquetInputReader();
        ParquetInputRecord record;

        for (File data : dataFiles) {
            parser.beginParsing(data);
            while ((record = parser.parseNextRecord()) != null) {
                try {
                    String venueName = record.getString("venue_name");
                    String baseName = record.getString("venue_name");
                    if (venueName == null || venueName.equals("")) {
                        throw new NullPointerException();
                    } else {
                        if (!venues.containsKey(venueName)) {
                            venues.put(venueName, new ArrayList<>());
                        }
                        List<Venue> relevant = venues.get(venueName);

                        boolean makeNew = true;

                        double recordLat = Double.parseDouble(record.getString("lat"));
                        double recordLon = Double.parseDouble(record.getString("lon"));

                        for (Venue existing : relevant) {
                            if (existing.distanceToThis(recordLat, recordLon) < 500) {
                                venueName = existing.getName();
                                makeNew = false;
                                break;
                            }
                        }

                        if (makeNew) {
                            String newName = baseName + "," + recordLat + "," + recordLon;
                            venueName = newName;
                            Venue newOne = new Venue(newName, recordLat, recordLon);
                            List<Venue> stats1 = venues.get(baseName);
                            stats1.add(newOne);
                            venues.put(baseName, stats1);
                        }
                        if (!uniqueData.containsKey(venueName)) {
                            uniqueData.put(venueName, new HashMap<>());
                            uniqueNumbers.put(venueName, 0);
                            recordCount.put(venueName, new HashMap<>());
                            dayData.put(venueName, new HashMap<>());
                            adIdData.put(venueName, new HashMap<>());
                            adIdDayData.put(venueName, new HashMap<>());
                            dwellStats.put(venueName, new HashMap<>());
                            bellDwellStats.put(venueName, new HashMap<>());
                        }
                    }
                }
                catch(NullPointerException e){

                }
            }
            parser.stopParsing();
        }
    }

    public static Venue findVenue(double locLat, double locLon){
        Venue target = null;
        for(List<Venue> lists: venues.values()){
            for(Venue ven: lists){
                if (ven.distanceToThis(locLat, locLon) < 500){
                    target = ven;
                    return target;
                }
            }
        }
        return target;
    }

    public static void main(String[] args) {
        File dataFolder = new File("./Data");
        File[] dataFiles = dataFolder.listFiles();
        ParquetInputReader parser = new ParquetInputReader();
        ParquetInputRecord record;

        makeVenues();
        for(File data: dataFiles){
            parser.beginParsing(data);
            while((record = parser.parseNextRecord()) != null){
                    try {
                        String venueName = record.getString("venue_name");
                        String baseName = record.getString("venue_name");
                        String dwell = record.getString("dwell_time");
                        boolean hasVenue = false;
                        if ((venueName == null || venueName.equals("")) && (dwell != null && !dwell.equals(""))) {
                            Venue foundVenue = findVenue(Double.parseDouble(record.getString("lat")), Double.parseDouble(record.getString("lat")));
                            if (foundVenue != null){
                                venueName = foundVenue.getName();
                                hasVenue = true;
                                added += 1;
                            }
                        }

                            List<Venue> relevant = venues.get(baseName);

                            double recordLat = Double.parseDouble(record.getString("lat"));
                            double recordLon = Double.parseDouble(record.getString("lon"));

                            if(!hasVenue) {
                                for (Venue existing : relevant) {
                                    if (existing.distanceToThis(recordLat, recordLon) < 500) {
                                        venueName = existing.getName();
                                        hasVenue = true;
                                        break;
                                    }
                                }
                            }

                            if(!hasVenue){
                                throw new NullPointerException();
                            }
                            long timeLong = Long.parseLong(record.getString("time"));
                            LocalDateTime dateTime;
                            LocalDate date;
                            try {
                                dateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(timeLong / 1000), ZoneId.of("-5"));
                                date = dateTime.toLocalDate();
                            } catch (NumberFormatException e) {
                                dateTime = LocalDateTime.MIN;
                                date = LocalDate.MIN;
                            }

                            if (date == LocalDate.MIN) {
                                if (!uniqueData.get(venueName).containsKey(LocalDate.MIN)) {
                                    uniqueData.get(venueName).put(LocalDate.MIN, new HashMap<>());
                                    adIdData.get(venueName).put(LocalDate.MIN, new HashMap<>());
                                }
                            } else if (!uniqueData.get(venueName).containsKey(date)) {
                                uniqueData.get(venueName).put(date, new HashMap<>());
                                dayData.get(venueName).put(date, new ArrayList<>());
                                adIdData.get(venueName).put(date, new HashMap<>());
                                adIdDayData.get(venueName).put(date, new ArrayList<>());
                                dwellStats.get(venueName).put(date, new ArrayList<>());
                                bellDwellStats.get(venueName).put(date, new ArrayList<>());
                            }

                            int hour;
                            if (date == LocalDate.MIN) {
                                hour = -1;
                            } else {
                                hour = dateTime.getHour();
                            }

                            if (date == LocalDate.MIN) {
                                if (!uniqueData.get(venueName).get(LocalDate.MIN).containsKey(hour)) {
                                    uniqueData.get(venueName).get(LocalDate.MIN).put(hour, new ArrayList<>());
                                    adIdData.get(venueName).get(LocalDate.MIN).put(hour, new ArrayList<>());
                                }
                            } else if (!uniqueData.get(venueName).get(date).containsKey(hour)) {
                                uniqueData.get(venueName).get(date).put(hour, new ArrayList<>());
                                adIdData.get(venueName).get(date).put(hour, new ArrayList<>());
                            }

                            long ad_id;
                            long token;
                            try {
                                if(record.getString("token") == null || record.getString("token").equals("")){
                                    token = 0L;
                                }
                                else{
                                    token = Long.parseLong(record.getString("token"));
                                }
                            } catch(NumberFormatException e){
                                throw new NullPointerException();
                            }
                            if(record.getString("advertiser_id_IDs") == null || record.getString("advertiser_id_IDs").equals("")){
                                ad_id = 0L;
                            }
                            else{
                                ad_id = Long.parseLong(record.getString("advertiser_id_IDs"));
                            }

                            if (!uniqueData.get(venueName).get(date).get(hour).contains(token)) {
                                uniqueData.get(venueName).get(date).get(hour).add(token);
                            }
                            if (!dayData.get(venueName).get(date).contains(token)){
                                dayData.get(venueName).get(date).add(token);
                            }

                            if (!adIdData.get(venueName).get(date).get(hour).contains(ad_id)) {
                                adIdData.get(venueName).get(date).get(hour).add(ad_id);
                            }

                            if (!adIdDayData.get(venueName).get(date).contains(ad_id)){
                                adIdDayData.get(venueName).get(date).add(ad_id);
                                uniqueNumbers.put(venueName, uniqueNumbers.get(venueName) + 1);
                            }
                            if(token != 0L){
                                if(!(record.getString("dwell_time") == null) && !record.getString("dwell_time").equals("")){
                                    bellDwellStats.get(venueName).get(date).add(Float.parseFloat(record.getString("dwell_time")));
                                }
                            }
                            if(!(record.getString("dwell_time") == null) && !record.getString("dwell_time").equals("")) {
                                dwellStats.get(venueName).get(date).add(Float.parseFloat(record.getString("dwell_time")));
                            }

                    }
                    catch(NullPointerException e){

                    }

                }
            files = files + 1;
            parser.stopParsing();
        }
        writeStats();
        try (FileWriter fw = new FileWriter("storeStats.csv")) {
            fw.write(statsText.toString());
        } catch (IOException e) {
            log.info("Error writing the csv statistics file. Error message: " + e.getMessage());
        }
    }

    private static double calculateAverage(List<Float> marks) {
        float sum = 0;
        if(!marks.isEmpty()) {
            for (Float mark : marks) {
                sum += mark;
            }
            return sum / marks.size();
        }
        return sum;
    }

    private static void writeStats(){
        statsText.append("Stats for location info based on venues: Info gathered from " + files + " files \n\n\n");

        statsText.append("aggregation field,venue_name,latitude,longitude,dwell(minutes),dwellnumbers,date,daily unique, hour 0, hour 1, hour 2, hour 3, hour 4, hour 5, hour 6, hour 7, hour 8, hour 9, hour 10,hour 11,hour 12,hour 13,hour 14,hour 15,hour 16,hour 17,hour 18,hour 19,hour 20,hour 21,hour 22,hour 23\n");
        Map<String, Integer> sortedMap = MapUtil.sortByValue(uniqueNumbers);
        List<Map.Entry<String,Integer>> entryList =
                new ArrayList<>(sortedMap.entrySet());
        DecimalFormat df = new DecimalFormat();
        df.applyPattern("###.#");
        for(int i = 1; i < 100; i++){
            Map.Entry<String, Integer> entry = entryList.get(entryList.size() - i);
            Set<LocalDate> unordered = uniqueData.get(entry.getKey()).keySet();
            List<LocalDate> ordered = new ArrayList<>(unordered);
            Collections.sort(ordered);
            for (LocalDate date: ordered){
                statsText.append("token,");
                statsText.append(entry.getKey() + ",");
                statsText.append(df.format(calculateAverage(bellDwellStats.get(entry.getKey()).get(date))) + ",");
                statsText.append(bellDwellStats.get(entry.getKey()).get(date).size() + ",");
                statsText.append(date.toString() + ",");
                statsText.append(dayData.get(entry.getKey()).get(date).size()+ ",");
                    for (int hours = 0; hours < 24; hours++){
                        if (uniqueData.get(entry.getKey()).get(date).keySet().contains(hours)){
                            statsText.append(uniqueData.get(entry.getKey()).get(date).get(hours).size() + ",");
                        }
                        else {
                            statsText.append(0 + ",");
                        }
                    }
                    statsText.append("\n");
            }
            unordered = adIdData.get(entry.getKey()).keySet();
            ordered = new ArrayList<>(unordered);
            Collections.sort(ordered);
            for (LocalDate date: ordered){
                statsText.append("ad_id,");
                statsText.append(entry.getKey() + ",");
                statsText.append(df.format(calculateAverage(dwellStats.get(entry.getKey()).get(date))) + ",");
                statsText.append(dwellStats.get(entry.getKey()).get(date).size() + ",");
                statsText.append(date.toString() + ",");
                statsText.append(adIdDayData.get(entry.getKey()).get(date).size()+ ",");
                for (int hours = 0; hours < 24; hours++){
                    if (adIdData.get(entry.getKey()).get(date).keySet().contains(hours)){
                        statsText.append(adIdData.get(entry.getKey()).get(date).get(hours).size() + ",");
                    }
                    else {
                        statsText.append(0 + ",");
                    }
                }
                statsText.append("\n");
            }
        }
    }

    private void createTokenMaps(List<String> tokenFiles){
        File tokenFolder = new File("TokenFiles");
        for(File token: tokenFolder.listFiles()){
            CsvParserSettings settings = new CsvParserSettings();
            settings.detectFormatAutomatically();
            CsvParser parser = new CsvParser(settings);

            parser.beginParsing(token);
            String[] headers = parser.parseNext();
            Map<String, String> tokenMap = new HashMap<>();
            String[] record;
            int i = 0;
            while ((record = parser.parseNext()) != null) {
                tokenMap.put(record[0].toLowerCase(), record[1]);
                i++;
            }
            parser.stopParsing();
        }
    }
}