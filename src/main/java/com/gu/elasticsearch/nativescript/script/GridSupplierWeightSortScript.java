package com.gu.elasticsearch.nativescript.script;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Collections;
import java.util.Optional;

import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.script.AbstractLongSearchScript;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import org.elasticsearch.node.Node;

import org.elasticsearch.index.fielddata.ScriptDocValues;


public class GridSupplierWeightSortScript extends AbstractLongSearchScript {

    public static class Factory extends AbstractComponent implements NativeScriptFactory {
        public static final String DEFAULT_DATE_SORT_FIELD = "uploadTime";
        public static final Map<String, Double> DEFAULT_SUPPLIER_WEIGHTS = Collections.emptyMap();

        @Inject
        public Factory(Node node, Settings settings) {
            super(settings);
        }

        private Map<String,Double> nodeMapStringDoubleValue(@Nullable Object node) {
            return Optional.ofNullable(node).map((Object n) -> {
                if(node instanceof Map<?,?>) {

                    @SuppressWarnings("unchecked")
                    Map<Object,Object> nieveMap = (Map<Object,Object>) n;
                    Map<String, Double> newMap = new HashMap<String, Double>();

                    Iterator<Map.Entry<Object,Object>> it = nieveMap.entrySet().iterator();
                    while (it.hasNext()) {
                        Map.Entry<Object, Object> pair = it.next();

                        Boolean keyIsString = pair.getKey() instanceof String;
                        Boolean valueIsDouble = pair.getValue() instanceof Double;

                        if (keyIsString && valueIsDouble) {
                            newMap.put((String) pair.getKey(), (Double) pair.getValue());
                        }
                        it.remove();
                    }

                    return newMap;
                } else {
                    return null;
                }
            }).orElse(DEFAULT_SUPPLIER_WEIGHTS);
        }

        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            Optional<Map<String, Object>> optionalParams = Optional.ofNullable(params);

            Optional<String> dateSortField =
                optionalParams.flatMap((Map<String, Object> p) ->
                    Optional.ofNullable(p.get("date_sort_field")).map((Object node) ->
                        XContentMapValues.nodeStringValue(node, null)));

            Optional<Map<String, Double>> supplierWeights =
                optionalParams.map((Map<String, Object> p) ->
                    nodeMapStringDoubleValue(p.get("supplier_weights")));

            return new GridSupplierWeightSortScript(
                    logger,
                    dateSortField.orElse(DEFAULT_DATE_SORT_FIELD),
                    supplierWeights.orElse(DEFAULT_SUPPLIER_WEIGHTS)
            );
        }
    }


    private final ESLogger logger;
    private final String dateSortField;
    private final Map<String, Double> supplierWeights;

    private GridSupplierWeightSortScript(
            ESLogger logger,
            String dateSortField,
            Map<String, Double> supplierWeights) {

        this.supplierWeights = supplierWeights;
        this.logger = logger;
        this.dateSortField = dateSortField;
    }

    @Override
    @SuppressWarnings("unchecked")
    public long runAsLong() {
        Optional<ScriptDocValues<String>> supplierValue =
            Optional.ofNullable((ScriptDocValues<String>) doc().get("supplier"));

        Optional<ScriptDocValues<Long>> dateValue =
            Optional.ofNullable((ScriptDocValues<Long>) doc().get("uploadTime"));

        final Optional<String> supplierFieldValue =
            supplierValue.map((ScriptDocValues<String> s) ->
                    ((ScriptDocValues.Strings) s).getValue());

        final Optional<Long> dateFieldValue =
            dateValue.map((ScriptDocValues<Long> d) ->
                    ((ScriptDocValues.Longs) d).getValue());

        final Double weight = 1D + supplierFieldValue.flatMap((String s) ->
                Optional.ofNullable(supplierWeights.get(s))).orElse(0D);

        return (long) (((double) dateFieldValue.orElse(0L)) * weight);
    }

}


