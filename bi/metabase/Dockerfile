FROM metabase/metabase:v0.55.7

RUN mkdir -p /plugins && \
    curl -L -o /plugins/pinot.metabase-driver.jar \
    https://github.com/startreedata/metabase-pinot-driver/releases/download/v1.1.0/pinot.metabase-driver-v1.1.0.jar

ENV MB_PLUGINS_DIR=/plugins
