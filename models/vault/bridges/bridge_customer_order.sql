{%- set yaml_metadata -%}
source_model: "HUB_CUSTOMER"
src_pk: "CUSTOMER_PK"
src_ldts: "LOAD_DATETIME"
as_of_dates_table: "V_AS_OF_DATES"
bridge_walk:
    CUSTOMER_ORDER:
        bridge_link_pk: "LINK_CUSTOMER_ORDER_PK"
        bridge_start_date: "EFF_SAT_CUSTOMER_ORDER_STARTDATE"
        bridge_end_date: "EFF_SAT_CUSTOMER_ORDER_ENDDATE"
        bridge_load_date: "EFF_SAT_CUSTOMER_ORDER_LOADDATE"
        link_table: "LINK_CUSTOMER_ORDER"
        link_pk: "CUSTOMER_ORDER_PK"
        link_fk1: "CUSTOMER_FK"
        link_fk2: "ORDER_FK"
        eff_sat_table: "EFF_SAT_CUSTOMER_ORDER"
        eff_sat_pk: "CUSTOMER_ORDER_PK"
        eff_sat_end_date: "END_DATE"
        eff_sat_load_date: "LOAD_DATETIME"
    ORDER_PRODUCT:
        bridge_link_pk: "LINK_ORDER_PRODUCT_PK"
        bridge_start_date: "EFF_SAT_ORDER_PRODUCT_STARTDATE"
        bridge_end_date: "EFF_SAT_ORDER_PRODUCT_ENDDATE"
        bridge_load_date: "EFF_SAT_ORDER_PRODUCT_LOADDATE"
        link_table: "LINK_ORDER_PRODUCT"
        link_pk: "ORDER_PRODUCT_PK"
        link_fk1: "ORDER_FK"
        link_fk2: "PRODUCT_FK"
        eff_sat_table: "EFF_SAT_ORDER_PRODUCT"
        eff_sat_pk: "ORDER_PRODUCT_PK"
        eff_sat_end_date: "END_DATE"
        eff_sat_load_date: "LOAD_DATETIME"
stage_tables_ldts:
    STG_CUSTOMER_ORDER: "LOAD_DATETIME"
    STG_ORDER_PRODUCT: "LOAD_DATETIME"
{%- endset -%}

{{ dbtvault.bridge(source_model=source_model, src_pk=src_pk,
                   bridge_walk=bridge_walk,
                   as_of_dates_table=as_of_dates_table,
                   stage_tables_ldts=stage_tables_ldts,src_ldts=src_ldts) }}