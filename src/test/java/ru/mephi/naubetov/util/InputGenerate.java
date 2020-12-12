package ru.mephi.naubetov.util;

public class InputGenerate {

    public static String getValidRecord() {
        String validRecord = "93074d8125fa8945c5a971c2374e55a8\t" +
                "20131019161502142\t" +
                "1\t" +
                "CAH9FYCtgQf\t" +
                "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 2.0.50727)\t" +
                "119.145.140.*\t" +
                "216\t" +
                "222\t" + //cityId
                "1\t" +
                "20fc675468712705dbf5d3eda94126da\t" +
                "9c1ecbb8a301d89a8d85436ebf393f7f\t" +
                "null\t" +
                "mm_10982364_973726_8930541\t" +
                "250\t" +
                "390\t" +
                "FourthView\t" +
                "Na\t" +
                "0\t" +
                "2259\t" +
                "294\t" + //bidding price
                "201\t" +
                "null\t" +
                "2259\t" +
                "0057,10059,10083,10102,10024,10006,10110,10031,10063,10116";

        return validRecord;
    }

    public static String getInvalidRecord() {
        String invalidRecord = "93074d8125fa8945c5a971c2374e55a8\t" +
                "20131019161502142\t" +
                "1\t" +
                "CAH9FYCtgQf\t" +
                "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 2.0.50727)\t" +
                "119.145.140.*\t" +
                "216\t" +
                "222\t" + //cityId
                "1\t" +
                "20fc675468712705dbf5d3eda94126da\t" +
                "9c1ecbb8a301d89a8d85436ebf393f7f\t" +
                "null\t" +
                "mm_10982364_973726_8930541\t" +
                "300\t" +
                "250\t" +
                "FourthView\t" +
                "Na\t" +
                "0\t" +
                "7323\t" +
                "234\t" + //binding price
                "201\t" +
                "null\t" +
                "2259\t" +
                "0057,10059,10083,10102,10024,10006,10110,10031,10063,10116";

        return invalidRecord;
    }
}
