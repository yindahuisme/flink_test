package com.yindahu;

import java.io.Serializable;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;

public class myTest implements Serializable {

    public static void main(String[] args) throws ParseException {
        SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date date=ft.parse("2020-04-01 01:00:00");
        System.out.print(date.getTime());
    }
}
