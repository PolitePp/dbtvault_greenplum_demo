{%- set yaml_metadata -%}
source_model: 'HUB_CUSTOMER'
src_pk: 'CUSTOMER_PK'
as_of_date_table: 'v_as_of_dates'
satellites:
    SAT_CUSTOMER_DETAILS:
      pk:
          'PK': 'CUSTOMER_PK'
      ldts:
          'LDTS': 'LOAD_DATE'
    SAT_CUSTOMER_LOGIN:
      pk:
          'PK': 'CUSTOMER_PK'
      ldts:
          'LDTS': 'LOAD_DATE'
    SAT_CUSTOMER_PROFILE:
      pk:
          'PK': 'CUSTOMER_PK'
      ldts:
          'LDTS': 'LOAD_DATE'
stage_tables:
    'STG_CUSTOMER_DETAILS': 'LOAD_DATE',
    'STG_CUSTOMER_LOGIN': 'LOAD_DATE',
    'STG_CUSTOMER_PROFILE': 'LOAD_DATE'
src_ldts: 'LOAD_DATE'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ dbtvault.pit(source_model=metadata_dict['source_model'],
                src_pk=metadata_dict['src_pk'],
                as_of_dates_table=metadata_dict['as_of_date_table'],
                satellites=metadata_dict['satellites'],
                stage_tables=metadata_dict['stage_tables'],
                src_ldts=metadata_dict['src_ldts']) }}