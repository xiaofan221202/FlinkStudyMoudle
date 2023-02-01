package com.tomtop.flink.A_Bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class HotItem {
    private Long itemId;
    private Long wEnd;
    private Long clickCount;
    
}
