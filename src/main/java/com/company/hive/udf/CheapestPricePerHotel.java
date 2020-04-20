package com.company.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.StructObject;
import org.apache.hadoop.hive.serde2.lazy.LazyArray;
import org.apache.hadoop.hive.serde2.lazy.LazyBoolean;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class CheapestPricePerHotel extends GenericUDF {

    private MapObjectInspector inputOI;
    //fast insert --> LinkedList
    private List<IntWritable> minCostList = new LinkedList<>();

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {

        //only one argument (column) check
        if (objectInspectors.length != 1) {
            throw new UDFArgumentException("Only one Argument is expected!");
        }
        //map<int,struct<advertisers:map<string,array<struct<eurocents:int,breakfast:boolean>>>>>
        //map object is expected
        ObjectInspector io = objectInspectors[0];
        if (io.getCategory() != ObjectInspector.Category.MAP) {
            throw new UDFArgumentException("Argument (Column) type should be map object");
        }
        inputOI = (MapObjectInspector) io;

        //List<Integer>(min cost per advertisers) will be returned
        return ObjectInspectorFactory
                .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        minCostList.clear();
        //map<int,struct<advertisers:map<string,array<struct<eurocents:int,breakfast:boolean>>>>>
        //map<int,struct<...>>
        Map<IntWritable, StructObject> hotels =
                (Map<IntWritable, StructObject>) inputOI.getMap(deferredObjects[0].get());

        //null check for hotelresults
        if (hotels == null) {
            return null;
        }

        for (StructObject advertisers : hotels.values()) {

            //null check for advertisers struct object
            if (advertisers == null) {
                return null;
            }

            //struct of advertisers
            StructObjectInspector structOI = (StructObjectInspector) inputOI.getMapValueObjectInspector();

            //IntWritable costTemp = null;
            IntWritable costTemp = new IntWritable(0);

            //list of advertiser's struct fields from advertisers Struct
            List<? extends StructField> advertiserFields = structOI.getAllStructFieldRefs();
            //map<string,array<struct<eurocents:int,breakfast:boolean>>>
            StructField advertiserStructField = advertiserFields.get(0);
            Object advertiserData = structOI.getStructFieldData(advertisers, advertiserStructField);
            //Map inspector for advertiser, advertisers:map<...>
            MapObjectInspector advertiserMapOI = (MapObjectInspector) advertiserStructField.getFieldObjectInspector();
            // Hadoop Text and Hadoop LazyArray on advertiser map
            Map<Text, LazyArray> advertiserMap = (Map<Text, LazyArray>) advertiserMapOI.getMap(advertiserData);

            for (LazyArray array : advertiserMap.values()) {
                //array<struct<eurocents:int,breakfast:boolean>>
                ListObjectInspector arrayOI = (ListObjectInspector) advertiserMapOI.getMapValueObjectInspector();
                List<StructObject> contentList = (List<StructObject>) arrayOI.getList(array);

                for (StructObject content : contentList) {
                    StructObjectInspector contentOI = (StructObjectInspector) arrayOI.getListElementObjectInspector();
                    List<? extends StructField> contentFields = contentOI.getAllStructFieldRefs();

                    //struct<eurocents:int,breakfast:boolean>
                    StructField eurocents = contentFields.get(0);
                    StructField breakfast = contentFields.get(1);

                    //SerDe
                    IntWritable cost = ((LazyInteger) contentOI.getStructFieldData(content, eurocents))
                            .getWritableObject();
                    BooleanWritable isBreakfast = ((LazyBoolean) contentOI.
                            getStructFieldData(content, breakfast)).getWritableObject();

                    if (isBreakfast.get()) {
                        if (costTemp.get() == 0 || cost.get() < costTemp.get()) {
                            costTemp = new IntWritable(cost.get());
                        }
                    }
                }
            }
            if (costTemp.get() != 0) {
                minCostList.add(costTemp);
            }
        }
        return (minCostList.isEmpty() ? null : minCostList);
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "";
    }
}
