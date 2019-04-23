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
public class SampleSubmitionDTO {
    private Optional<Integer> id;
    private Optional<String> hotelCluster;
}
