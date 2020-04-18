package com.company.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

public class CheapestPricePerHotelTest {

    @Test(expected = UDFArgumentException.class)
    public void testArgNull() throws Exception {
        try (CheapestPricePerHotel udf = new CheapestPricePerHotel()) {
            ObjectInspector[] arguments = {};
            udf.initialize(arguments);
        }
    }

    @Test(expected = UDFArgumentException.class)
    public void testArgCorrectType() throws Exception {
        try (CheapestPricePerHotel udf = new CheapestPricePerHotel()) {
            ObjectInspector inputOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
            ObjectInspector[] arguments = {inputOI};
            udf.initialize(arguments);
        }
    }

    @Test(expected = UDFArgumentException.class)
    public void testArgMoreThanOne() throws Exception {
        try (CheapestPricePerHotel udf = new CheapestPricePerHotel()) {
            ObjectInspector inputOI1 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
            ObjectInspector inputOI2 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
            ObjectInspector[] arguments = {inputOI1, inputOI2};
            udf.initialize(arguments);
        }
    }
}