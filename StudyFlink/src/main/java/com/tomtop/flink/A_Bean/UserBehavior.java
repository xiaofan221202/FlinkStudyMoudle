package com.tomtop.flink.A_Bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserBehavior {
    private Long userId;
    private Long itemId;  // 商品id
    private Integer categoryId;
    private String behavior;
    private Long timestamp;
}
