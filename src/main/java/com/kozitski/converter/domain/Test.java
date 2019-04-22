package com.kozitski.converter.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Test {
    private String id;
    private String date_time;
    private int site_name;
    private int posa_continent;
    private int user_location_country;
    private int user_location_region;
    private int user_location_city;
    private double orig_destination_distance;
    private int user_id;
    private int is_mobile;
    private int is_package;
    private int channel;
    private String srch_ci;
    private String srch_co;
    private int srch_adults_cnt;
    private int srch_children_cnt;
    private int srch_rm_cnt;
    private int srch_destination_id;
    private int srch_destination_type_id;
    private int hotel_continent;
    private int hotel_country;
    private int hotel_market;
//    private int is_booking;
//    private long cnt;
//    private int hotel_cluster;
}
