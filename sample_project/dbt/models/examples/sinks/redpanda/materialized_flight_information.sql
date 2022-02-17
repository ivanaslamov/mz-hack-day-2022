{{ config(materialized='sink') }}

{% set sink_name %}
    {{ mz_generate_name('redpanda_flight_information') }}
{% endset %}

CREATE SINK {{ sink_name }}
FROM {{ ref('rp_flight_information') }} INTO KAFKA BROKER 'redpanda:9092' TOPIC 'materialized_rp_flight_information' FORMAT JSON;
