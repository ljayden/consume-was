package com.uneedcomms.ustream.consumer.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uneedcomms.ustream.consumer.common.CsvUtils;
import com.uneedcomms.ustream.consumer.common.DateUtils;
import com.uneedcomms.ustream.consumer.common.JsonUtils;
import com.uneedcomms.ustream.consumer.schema.KafkaMessageKey;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

@Component
@KafkaListener(id = "common-topic", topics = {"common"})
public class CommonListener {

    Logger logger = LoggerFactory.getLogger(CommonListener.class);

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    DateUtils dateUtils;

    @Autowired
    CsvUtils csvUtils;

    @Autowired
    JsonUtils jsonUtils;

    // 각 target(appId+eventName) 을 모아논 Collection
    private static HashSet<String> targets = new HashSet<>();
    // 각 target(appId+eventName) 별로 모여진 row 객체의 리스트
    private static HashMap<String, ArrayList<HashMap<String, Object>>> rowListMap = new HashMap<>();
    // 각 target(appId+eventName) 별로 헤더(row 의 키값)만 모아논 String Set
    private static HashMap<String, LinkedHashSet<String>> configs = new HashMap();
    // 각 target(appId+eventName) 별로 해더+바디 정렬된 csv format string HashSet;
    private static HashMap<String, String> csvs = new HashMap<>();

    /**
     * limit count
     */
    @Value("${limit.row}")
    private int limitRow;

    /**
     * count index
     */
    private int count = 0;


    /**
     * Kafka Consumer
     * Topic "common" Listener
     *
     * @param in
     * @throws IOException
     */
    @KafkaHandler
    public void takeMessage(String in) throws IOException {
        categorization(in);
    }

    /**
     * categorization
     *
     * @param in
     * @throws
     */
    private void categorization(String in) throws IOException {
        this.count++;
        HashMap<String, Object> row = objectMapper.readValue(in, HashMap.class);

        String appId = KafkaMessageKey.SW_APP_ID.toString();
        String eventName = KafkaMessageKey.SW_EVENT_NAME.toString();
        if(!row.containsKey(appId) && !row.containsKey(eventName)) throw new RuntimeException();

        String target = row.get(appId) + "_" + row.get(eventName);
        String fileName = target + "_" + dateUtils.getLocalDateTime() + ".csv";
        targets.add(target);

        System.out.println("No. "+count+", target: "+fileName);

        ArrayList<HashMap<String, Object>> rowList = new ArrayList<>();

        // appId_eventName 으로 존재하는 리스트 검색
        if(rowListMap.containsKey(target)) rowList = rowListMap.get(target);

        // 리스트에 열 추가한후 맵에 저장
        rowList.add(row);
        rowListMap.put(target, rowList);


        // config 정보에 target의 header 정보 추가
        LinkedHashSet<String> headers = new LinkedHashSet<>();
        if(configs.containsKey(target)) headers = configs.get(target);
        for (String key: row.keySet()) {
            if(!headers.contains(key)) headers.add(key);
        }
        configs.put(target, headers);

        headers.stream().forEach(e-> System.out.print(e+", "));
        System.out.println();

        System.out.println(target+"의 size: "+rowList.size());
        rowList.stream().forEach(System.out::println);
        System.out.println(target+"의 해더 size: "+configs.get(target).size());

        System.out.println();
        System.out.println();

        // limitRow 발동시 진행될 로직
        if(limitRow <= this.count) {
            convertToCsv();
            // 메모리 정리 및 카운트 0 세팅
            polishingStatic();
        }
    }

    /**
     * static map 에 저장된 list를 csv format으로 변환
     *
     */
    private void convertToCsv() {

        // limit row 까지 쌓인 target 의 수
        HashSet<String> targets = this.targets;
        System.out.println("targets 들: "+targets);
        // limit row 까지 쌓인 target 의 header값들
        Map<String, LinkedHashSet<String>> configs = this.configs;
        System.out.println("configs의 사이즈: "+configs.size());
        // limit row 까지 쌓인 target 별 row 객체의 list
        HashMap<String, ArrayList<HashMap<String, Object>>> rowListMap = this.rowListMap;
        System.out.println("rowListMap 의 사이즈: "+rowListMap.size());

        // 설정과 맵리스트 같지 않으면 throw
        if(targets.size() != configs.size() && configs.size() != rowListMap.size()) throw new RuntimeException();



        String csvHeader = new String();
        String csvBody = new String();
        // csv 생성
        for (String target: targets) {
            LinkedHashSet<String> header = configs.get(target);
            ArrayList<HashMap<String, Object>> rowList = rowListMap.get(target);

            // csv header 생성
            for (String column : header) csvHeader += column + ",";
            csvHeader = csvHeader.substring(0, csvHeader.length()-1) + "\n";

            // csv body 생성
            for (int i = 0; i < rowList.size(); i++) {
                HashMap<String, Object> row = rowList.get(i);
                String oneRow = "";
                // header 생성 및 컬럼 일치여부 체크
                int index = 0;
                for(String column: header) {
                    if (index == header.size() - 1) {
                        if (row.get(column) == null) {
                            oneRow += "null\n";
                        } else {
                            oneRow += row.get(column) + "\n";
                        }
                    } else {
                        if (row.get(column) == null) {
                            oneRow += "null,";
                        } else {
                            oneRow += row.get(column) + ",";
                        }
                    }
                    index++;
                }
                csvBody += oneRow;
            }

            String targetCsv = csvHeader + csvBody;
            csvs.put(target, targetCsv);
        }

        // file IO
        try {
            fileOut(csvs);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 파라미터로 받아온 target(appId+eventName) 별 csv string 을 파일로 Out
     * @param csvs
     * @throws IOException
     */
    private void fileOut(HashMap<String, String> csvs) throws IOException {
        for (String key: csvs.keySet()) {
            csvUtils.writeToFile(csvs.get(key),key + "_" + dateUtils.getLocalDateTime() + ".csv");
        }
    }

    /**
     * 정해진 index 만큼 쌓은 후 메모리 비워주는 작업
     */
    private void polishingStatic() {

        this.targets.clear();
        this.rowListMap.clear();
        this.configs.clear();
        this.count = 0;

    }

}
