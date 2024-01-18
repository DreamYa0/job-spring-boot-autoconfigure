package com.g7.framework.job.util;


import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 日期工具类
 */
public class DateUtils {

    /** 年月日时分秒(无下划线) yyyyMMddHHmmss */
    private static final String YYYY_MMDD_HHMMSS = "yyyyMMddHHmmss";

    /**
     * Formats a Date into a date/time string.
     * @param date
     * @param pattern 格式 yyyyMMddHHmmss / yyMMdd /...
     * @return
     */
    public static String format(Date date, String pattern) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        return sdf.format(date);
    }

    /**
     * Parses text from the beginning of the given string to produce a date.
     * @param date    日期字符串
     * @param pattern 格式 yyyyMMddHHmmss / yyMMdd /...
     * @return
     * @throws ParseException
     */
    public static Date parse(String date, String pattern) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        try {
            Date d = sdf.parse(date);
            return d;
        } catch (ParseException e) {
            throw new RuntimeException("日期转换错误", e);
        }

    }

    /**
     * add(Calendar.DAY_OF_MONTH, -5)
     * @param date
     * @param calendorField
     * @param amount
     * @return
     */
    public static Date add(Date date, int calendorField, int amount) {
        Calendar cal = Calendar.getInstance();

        cal.setTime(date);

        cal.add(calendorField, amount);

        return cal.getTime();
    }

    /**
     * @return Calendar.SUNDAY <br/>
     * Calendar.MONDAY <br/>
     * Calendar.TUESDAY <br/>
     * Calendar.WEDNESDAY <br/>
     * Calendar.THURSDAY <br/>
     * Calendar.FRIDAY <br/>
     * Calendar.SATURDAY <br/>
     */
    public static int getDayOfWeek(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        switch (cal.get(Calendar.DAY_OF_WEEK)) {
            case 1:
                return 7;
            case 2:
                return 1;
            case 3:
                return 2;
            case 4:
                return 3;
            case 5:
                return 4;
            case 6:
                return 5;
            default:
                return 6;
        }
    }

    /**
     * @return Calendar.SUNDAY <br/>
     * Calendar.MONDAY <br/>
     * Calendar.TUESDAY <br/>
     * Calendar.WEDNESDAY <br/>
     * Calendar.THURSDAY <br/>
     * Calendar.FRIDAY <br/>
     * Calendar.SATURDAY <br/>
     */
    public static int getDayOfMouth(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        return cal.get(Calendar.DAY_OF_MONTH);
    }

    /**
     * 计算两个日期间相差的天数
     * @param date
     * @param compareDate
     * @return
     * @throws ParseException
     */
    public static long compareTo(Date date, Date compareDate) {
        // 去掉时分秒
        date = parse(format(date, "yyMMdd"), "yyMMdd");
        compareDate = parse(format(compareDate, "yyMMdd"), "yyMMdd");

        long a = (date.getTime() - compareDate.getTime()) / (1000 * 60 * 60 * 24);
        return a;
    }

    /**
     * 判断是否为一周的最后一天(目前配置的是周日为一周的第一天)
     * @param date
     * @return
     */
    public static boolean isEndOfWeek(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.DAY_OF_MONTH, 1);
        int weekDay = cal.get(Calendar.DAY_OF_WEEK);
        int FIRST_DATE_OF_WEEK = Calendar.SUNDAY;
        if (weekDay == FIRST_DATE_OF_WEEK) {
            return true;
        }
        return false;
    }

    /**
     * 判断时间是否为月末
     * @param nowDate 日期（需要验证的日期）
     * @return boolean true 表示是月末 false 表示不为月末
     */
    public static boolean isMonthEnd(Date nowDate) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(nowDate);
        cal.add(Calendar.DAY_OF_MONTH, 1);
        int day = cal.get(Calendar.DAY_OF_MONTH);
        if (day == 1) {
            return true;
        }
        return false;
    }

    /**
     * 判断时间是否为月初
     * @param nowDate
     * @return
     */
    public static boolean isMonthBegin(Date nowDate) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(nowDate);
        int day = cal.get(Calendar.DAY_OF_MONTH);
        if (day == 1) {
            return true;
        }
        return false;
    }

    /**
     * 得到日期的年月
     * @return
     */
    public static String findYearMonth(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH) + 1;
        String dateMouth = year + "-" + (month < 10 ? "0" + month : month);
        return dateMouth;
    }

    /**
     * 判断时间是否为季末
     * @param nowDate 日期（需要验证的日期）
     * @return boolean true 表示是季末 false 表示不是季末
     */
    public static boolean isQuarterEnd(Date nowDate) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(nowDate);
        int month = cal.get(Calendar.MONTH);
        cal.add(Calendar.DAY_OF_MONTH, 1);
        int day = cal.get(Calendar.DAY_OF_MONTH);
        if (day == 1
                && (month == Calendar.MARCH || month == Calendar.JUNE || month == Calendar.SEPTEMBER || month == Calendar.DECEMBER)) {
            return true;
        }
        return false;
    }

    /**
     * 判断时间是否为季出
     * @param nowDate 日期（需要验证的日期）
     * @return boolean true 表示是季初 false 表示不是季初
     */
    public static boolean isQuarterBegin(Date nowDate) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(nowDate);
        int month = cal.get(Calendar.MONTH);
        int day = cal.get(Calendar.DAY_OF_MONTH);
        if (day == 1
                && (month == Calendar.JANUARY || month == Calendar.APRIL || month == Calendar.JULY || month == Calendar.OCTOBER)) {
            return true;
        }
        return false;
    }

    /**
     * 判断时间是否为半年末
     * @param nowDate 日期（需要验证的日期）
     * @return boolean true 表示是半年末 false 表示不是半年末
     */
    public static boolean isHalfYearEnd(Date nowDate) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(nowDate);
        int month = cal.get(Calendar.MONTH);
        cal.add(Calendar.DAY_OF_MONTH, 1);
        int day = cal.get(Calendar.DAY_OF_MONTH);
        if (day == 1 && (month == Calendar.JUNE || month == Calendar.DECEMBER)) {
            return true;
        }
        return false;
    }

    /**
     * 判断时间是否为半年出
     * @param nowDate 日期（需要验证的日期）
     * @return boolean true 表示是半年初 false 表示不是半年初
     */
    public static boolean isHalfYearBegin(Date nowDate) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(nowDate);
        int month = cal.get(Calendar.MONTH);
        int day = cal.get(Calendar.DAY_OF_MONTH);
        if (day == 1 && (month == Calendar.JANUARY || month == Calendar.JULY)) {
            return true;
        }
        return false;
    }

    /**
     * 判断时间是否为年末
     * @param nowDate 日期（需要验证的日期）
     * @return boolean true 表示是年末 false 表示不为年末
     */
    public static boolean isYearEnd(Date nowDate) {
        if ("1231".equals(format(nowDate, "MMdd"))) {
            return true;
        }
        return false;
    }

    /**
     * 判断时间是否为年初
     * @param nowDate 日期（需要验证的日期）
     * @return boolean true 表示是年初 false 表示不为年初
     */
    public static boolean isYearBegin(Date nowDate) {
        if ("0101".equals(format(nowDate, "MMdd"))) {
            return true;
        }
        return false;
    }

    /**
     * 获取日期的年月日
     * @return
     */
    public static Calendar getYMD(Date date) {
        String dateStr = format(date, "yyyyMMdd");
        date = parse(dateStr, "yyyyMMdd");
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar;
    }

    /**
     * 判断是否为结算日期
     * @param stlCycle ,stlCycleDay,tranDate
     * @return boolean
     */
    public static boolean chkStlTime(String stlCycle, String stlCycleDay, Date tranDate) {
        boolean b = false;
        switch (stlCycle.toCharArray()[0]) {
            case '1':
                // 日结
                b = true;
                break;
            case '2':
                // 周结
                String nowDate = String.valueOf(DateUtils.getDayOfWeek(tranDate));
                if (nowDate.equals(stlCycleDay)) {
                    b = true;
                }
                break;
            case '3':
                // 月结
                if (stlCycleDay.equals("0")) {
                    // 月末结
                    boolean result = DateUtils.isMonthEnd(tranDate);
                    if (result) {
                        b = true;
                    }
                } else {
                    // 非月末结
                    String nowDate1 = String.valueOf(DateUtils.getDayOfMouth(tranDate));
                    if (nowDate1.equals(stlCycleDay)) {
                        b = true;
                    }
                }
                break;
            case '4':
                // 季结
                if ("1".equals(stlCycleDay)) {
                    // 季初
                    if (DateUtils.isQuarterBegin(tranDate)) {
                        b = true;
                    }
                } else if ("0".equals(stlCycleDay)) {
                    // 季末
                    if (DateUtils.isQuarterEnd(tranDate)) {
                        b = true;
                    }
                }
                break;
            case '5':
                // 半年结
                if ("1".equals(stlCycleDay)) {
                    // 半年初
                    if (DateUtils.isHalfYearBegin(tranDate)) {
                        b = true;
                    }
                } else if ("0".equals(stlCycleDay)) {
                    // 半年末
                    if (DateUtils.isHalfYearEnd(tranDate)) {
                        b = true;
                    }
                }
                break;
            case '6':
                // 年结
                if ("1".equals(stlCycleDay)) {
                    // 年初
                    if (DateUtils.isYearBegin(tranDate)) {
                        b = true;
                    }
                } else if ("0".equals(stlCycleDay)) {
                    // 年末
                    if (DateUtils.isYearEnd(tranDate)) {
                        b = true;
                    }
                }
                break;
            default:
                break;
        }
        return b;
    }

    //比较频繁交易前后两笔的时间间隔与指定的某个时间对比，在这个时间段内，是频繁交易
    public static boolean monFreCompare(Date startTime, Date endTime, int interTime) {
        boolean flag = false;
        long a = (endTime.getTime() - startTime.getTime());
        //两笔交易的时间间隔<=interTime,是频繁交易
        long interval = a / 1000;
        if (interval <= interTime && interval > 0) {
            flag = true;
        }

        return flag;
    }

    /**
     * 比较两个日期是否为同一天
     * @param firstDate
     * @param secondDate
     * @return
     */
    public static boolean judgeDate(Date firstDate, Date secondDate) {
        Calendar calFirst = Calendar.getInstance();
        calFirst.setTime(firstDate);

        Calendar calSecond = Calendar.getInstance();
        calSecond.setTime(secondDate);

        if (calFirst.compareTo(calSecond) == 0) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 获取区间为半小时的数据
     * @param diff
     * @param minute
     * @return
     */
    public static final String getDiffDateTime(int diff, int minute) {
        Calendar c = Calendar.getInstance();
        c.setTime(new Date());
        c.add(Calendar.DATE, diff);
        c.add(Calendar.MINUTE, minute);

        return getFormat(YYYY_MMDD_HHMMSS).format(c.getTime());
    }

    /**
     * 获取格式
     * @param format
     * @return
     */
    public static final DateFormat getFormat(String format) {
        return new SimpleDateFormat(format);
    }

    //获取两个时间之间相差的天数
    public static int betweenDays(Date nowDate, Date LastLogpwdModifytime) {
        int result = 0;
        try {
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            SimpleDateFormat df1 = new SimpleDateFormat("yyyy-MM-dd");
            Calendar cal = Calendar.getInstance();
            cal.setTime(df.parse(df.format(nowDate)));
            long time1 = cal.getTimeInMillis();
            Calendar cal1 = Calendar.getInstance();
            cal1.setTime(df1.parse(df1.format(LastLogpwdModifytime)));
            long time2 = cal1.getTimeInMillis();
            result = Integer.parseInt(String.valueOf((time1 - time2) / (1000 * 60 * 60 * 24)));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return result;
    }


    public static void main(String[] args) {
        Calendar postCalendar = Calendar.getInstance();
        postCalendar.set(Calendar.YEAR, 2005);
        postCalendar.set(Calendar.MONTH, 2);
        postCalendar.set(Calendar.DAY_OF_MONTH, 1);
//		postCalendar.set(Calendar.HOUR, 13);
//		postCalendar.set(Calendar.MINUTE, 20);
//		postCalendar.set(Calendar.SECOND, 13);

        Calendar preCalendar = Calendar.getInstance();
        preCalendar.set(Calendar.YEAR, 2005);
        preCalendar.set(Calendar.MONTH, 2);
        preCalendar.set(Calendar.DAY_OF_MONTH, 1);
        System.out.println(DateUtils.format(new Date(), "yyyyMMddhhmmssSSS"));
//		preCalendar.set(Calendar.HOUR, 11);
//		preCalendar.set(Calendar.MINUTE, 26);
//		preCalendar.set(Calendar.SECOND, 25);

//		long times = betweenTimes(postCalendar.getTime(), preCalendar.getTime());
//		System.out.println(times/3600 + "小时" + (times/60 - times/3600 * 60) + "分钟" + times%60 + "秒");

//		System.out.println(DateUtils.format(preCalendar.getTime(), "yyyy-MM-dd HH:mm:ss"));
//		System.out.println(DateUtils.format(postCalendar.getTime(), "yyyy-MM-dd HH:mm:ss"));
//		System.out.println(DateUtils.format(DateUtils.getRealTime(preCalendar.getTime(), "1"), "yyyy-MM-dd HH:mm:ss"));
//		System.out.println(DateUtils.format(DateUtils.getRealTime(postCalendar.getTime(), "2"), "yyyy-MM-dd HH:mm:ss"));

    }
}
