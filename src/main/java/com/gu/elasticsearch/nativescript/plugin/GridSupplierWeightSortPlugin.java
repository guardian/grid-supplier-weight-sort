package com.gu.elasticsearch.nativescript.plugin;

import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.script.ScriptModule;


public class GridSupplierWeightSortPlugin extends AbstractPlugin {
    @Override
    public String name() {
        return "grid-supplier-weight-sort";
    }

    @Override
    public String description() {
        return "Elasticsearch Native Plugin for Guardian Grid image supplier weighted sort";
    }
}
