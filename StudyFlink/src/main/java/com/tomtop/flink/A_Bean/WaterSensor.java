package com.tomtop.flink.A_Bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data  /*生成setter 和 getter方法 toString  hashCode  equals*/
@NoArgsConstructor /*生成无参构造器*/
@AllArgsConstructor /*生成有参构造器*/
public class WaterSensor {
    private String id;
    private Long ts;
    private Integer vc;
}
