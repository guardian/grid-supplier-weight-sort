package com.gu.elasticsearch.nativescript.script;

import java.util.Map;
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

        @Inject
        public Factory(Node node, Settings settings) {
            super(settings);
        }

        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            Optional<Map<String, Object>> optionalParams = Optional.ofNullable(params);

            Optional<String> dateSortField =
                optionalParams.flatMap((Map<String, Object> p) ->
                    Optional.ofNullable(p.get("date_sort_field")).map((Object node) ->
                        XContentMapValues.nodeStringValue(node, null)));

            // TODO: This isn't a nullable type - but the eventual impl here will be.
            Optional<Map<String, Float>> supplierWeighting =
                Optional.ofNullable(Collections.emptyMap());

            return new GridSupplierWeightSortScript(
                    logger,
                    dateSortField.orElse(DEFAULT_DATE_SORT_FIELD)
            );
        }
    }


    private final ESLogger logger;
    private final String dateSortField;

    private GridSupplierWeightSortScript(ESLogger logger, String dateSortField) {
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

        return dateFieldValue.orElse(0L);
    }

}


