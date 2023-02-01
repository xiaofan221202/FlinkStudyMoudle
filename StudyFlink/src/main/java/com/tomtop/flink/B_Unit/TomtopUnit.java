package com.tomtop.flink.B_Unit;


import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author:txf
 * @Date:2022/12/11 19:54
 */
public class TomtopUnit {

    //这个方法把时间转成一个yyyy-MM-dd HH:mm:ss格式的字符串
    public static String toDateTime(long ts){
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ts);
    }

    //这个方法就把迭代器中的元素集合转成一个list集合。原因iterable中元素处理不方便，比如不能排序。
    public static <T> List<T> toList(Iterable<T> it) {
        ArrayList<T> list = new ArrayList<>();
        for (T t : it) {
            list.add(t);
        }
        return list;
    }
}
