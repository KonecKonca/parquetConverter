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
    private Optional<String> date_time;
    private Optional<Integer> site_name;
    private Optional<Integer> posa_continent;
    private Optional<Integer> user_location_country;
    private Optional<Integer> user_location_region;
    private Optional<Integer> user_location_city;
    private Optional<Double> orig_destination_distance;
    private Optional<Integer> user_id;
    private Optional<Integer> is_mobile;
    private Optional<Integer> is_package;
    private Optional<Integer> channel;
    private Optional<String> srch_ci;
    private Optional<String> srch_co;
    private Optional<Integer> srch_adults_cnt;
    private Optional<Integer> srch_children_cnt;
    private Optional<String> srch_rm_cnt;
    private Optional<Integer> srch_destination_id;
    private Optional<Integer> srch_destination_type_id;
    private Optional<Integer> hotel_continent;
    private Optional<Integer> hotel_country;
    private Optional<Integer> hotel_market;
//    private Integereger is_booking;
//    private long cnt;
//    private Integer hotel_cluster;
    
}
