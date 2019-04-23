package com.kozitski.converter.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Optional;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TestDTO {
    private Optional<String> id;
    private Optional<String> dateTime;
    private Optional<Integer> siteName;
    private Optional<Integer> posaContinent;
    private Optional<Integer> userLocationCountry;
    private Optional<Integer> userLocationRegion;
    private Optional<Integer> userLocationCity;
    private Optional<Double> origDestinationDistance;
    private Optional<Integer> userId;
    private Optional<Integer> isMobile;
    private Optional<Integer> isPackage;
    private Optional<Integer> channel;
    private Optional<String> srchCi;
    private Optional<String> srchCo;
    private Optional<Integer> srchAdultsCnt;
    private Optional<Integer> srchChildrenCnt;
    private Optional<String> srchRmCnt;
    private Optional<Integer> srchDestinationId;
    private Optional<Integer> srchDestinationTypeId;
    private Optional<Integer> hotelContinent;
    private Optional<Integer> hotelCountry;
    private Optional<Integer> hotelMarket;
}
