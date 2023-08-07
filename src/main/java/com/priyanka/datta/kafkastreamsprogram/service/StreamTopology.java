package com.priyanka.datta.kafkastreamsprogram.service;

import com.priyanka.datta.kafkastreamsprogram.dto.CustomDeSerializer;
import com.priyanka.datta.kafkastreamsprogram.dto.CustomDto;
import com.priyanka.datta.kafkastreamsprogram.dto.CustomSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class StreamTopology {

    static CustomSerializer<CustomDto> customSerializer = new CustomSerializer();
    static CustomDeSerializer<CustomDto> customDeSerializer = new CustomDeSerializer();
    static Serde<CustomDto> serde = Serdes.serdeFrom(customSerializer,customDeSerializer);


    public static Topology createTopology(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        /*KStream<String,String> kStream = streamsBuilder.stream("test",Consumed.with(Serdes.String(),Serdes.String()))
                .filter((key,value)->value.length()>5)
                .map((key,value) -> KeyValue.pair(key,value.toUpperCase()));
        kStream.to("test2",Produced.with(Serdes.String(),Serdes.String()));*/
        /*KTable<String, String> kTable = streamsBuilder.stream("test",Consumed.with(Serdes.String(),Serdes.String()))
                .flatMapValues(val->Arrays.asList(val.split("")))
                .toTable();                        ;
        kTable.toStream().to("test2",Produced.with(Serdes.String(),Serdes.String()));*/
        KStream<String,CustomDto> kStream = streamsBuilder.stream("test",Consumed.with(Serdes.String(),serde));
        KStream<String,CustomDto>[] kStreamList = kStream.branch((key,value)-> value.getId()%2==0,
                        (key,value) -> value.getId()%2!=0);
        kStreamList[0].mapValues(CustomDto::toString).to("test2",Produced.with(Serdes.String(),Serdes.String()));
        kStreamList[1].mapValues(CustomDto::toString).to("test3",Produced.with(Serdes.String(),Serdes.String()));
        //KTable<String, Long> kTable = kStream.toTable().groupBy(Grouped.with(Serdes.String(),Serdes.String())).count().toStream();

        //kStream.to("test2",Produced.with(Serdes.String(),Serdes.String()));
        kStream.print(Printed.toSysOut());
        return streamsBuilder.build();
    }
}
