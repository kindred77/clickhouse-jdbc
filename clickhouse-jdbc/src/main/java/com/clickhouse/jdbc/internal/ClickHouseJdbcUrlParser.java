package com.clickhouse.jdbc.internal;

import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Properties;

import com.clickhouse.client.*;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.client.logging.Logger;
import com.clickhouse.client.logging.LoggerFactory;
import com.clickhouse.jdbc.ClickHouseDriver;
import com.clickhouse.jdbc.JdbcConfig;
import com.clickhouse.jdbc.SqlExceptionUtils;

public class ClickHouseJdbcUrlParser {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseJdbcUrlParser.class);

    public static class ConnectionInfo {
        private final String cacheKey;
        private final ClickHouseCredentials credentials;
        private final ClickHouseNodes nodes;
        private final JdbcConfig jdbcConf;
        private final Properties props;

        protected ConnectionInfo(String cacheKey, ClickHouseNodes nodes, Properties props) {
            this.cacheKey = cacheKey;
            this.nodes = nodes;
            this.jdbcConf = new JdbcConfig(props);
            this.props = props;

            ClickHouseCredentials c = nodes.getTemplate().getCredentials().orElse(null);
            if (props != null && !props.isEmpty()) {
                String user = props.getProperty(ClickHouseDefaults.USER.getKey(), "");
                String passwd = props.getProperty(ClickHouseDefaults.PASSWORD.getKey(), "");
                if (!ClickHouseChecker.isNullOrEmpty(user)) {
                    c = ClickHouseCredentials.fromUserAndPassword(user, passwd);
                }
            }
            this.credentials = c;
        }

        public ClickHouseCredentials getDefaultCredentials() {
            return this.credentials;
        }

        /**
         * Gets selected server.
         *
         * @return non-null selected server
         * @deprecated will be removed in v0.3.3, please use {@link #getNodes()}
         *             instead
         */
        @Deprecated
        public ClickHouseNode getServer() {
            return nodes.apply(nodes.getNodeSelector());
        }

        public JdbcConfig getJdbcConfig() {
            return jdbcConf;
        }

        /**
         * Gets nodes defined in connection string.
         *
         * @return non-null nodes
         */
        public ClickHouseNodes getNodes() {
            return nodes;
        }

        public Properties getProperties() {
            return props;
        }
    }

    // URL pattern:
    // jdbc:(clickhouse|ch)[:(grpc|http|tcp)]://host[:port][/db][?param1=value1&param2=value2]
    public static final String JDBC_PREFIX = "jdbc:";
    public static final String JDBC_CLICKHOUSE_PREFIX = JDBC_PREFIX + "clickhouse:";
    public static final String JDBC_ABBREVIATION_PREFIX = JDBC_PREFIX + "ch:";
    public static final String TRANS_TO_MULTI_DIRECT_CONN = "trans_to_multi_direct_conn";

    static Properties newProperties() {
        Properties props = new Properties();
        props.setProperty(ClickHouseClientOption.ASYNC.getKey(), Boolean.FALSE.toString());
        props.setProperty(ClickHouseClientOption.FORMAT.getKey(), ClickHouseFormat.RowBinaryWithNamesAndTypes.name());
        return props;
    }

    public static String SQL_GET_HOST_PORT_IN_CLUSTER = "select distinct host_address,http_port from system.clusters where host_address not like '%::1%' order by rand()";
    private static String getNewJDBCUrl(String jdbcUrl, Properties defaults) throws SQLException
    {
        log.info("Input jdbcURL is: %s",jdbcUrl);
        String newJdbcUrlPrefix="http://";
        String tmp=jdbcUrl.substring(jdbcUrl.indexOf("://")+4);
        int idxSlash =tmp.indexOf("/");
        final String newJdbcUrlSuffix;
        if(idxSlash>0)
        {
            newJdbcUrlSuffix=tmp.substring(tmp.indexOf("/"));//include '/'
        }
        else
        {
            int idxQuest =tmp.indexOf("?");
            if(idxQuest>0)
            {
                newJdbcUrlSuffix=tmp.substring(idxQuest);//include '?'
            }
            else
            {
                newJdbcUrlSuffix="";
            }
        }
        log.debug("New JdbcUrl Suffix is: %s", newJdbcUrlSuffix);
        final ClickHouseRequest<?> clientRequest;
        try {
            String cacheKey = ClickHouseNodes.buildCacheKey(jdbcUrl, defaults);
            ClickHouseNodes nodes = ClickHouseNodes.of(cacheKey, jdbcUrl, defaults);

            Properties props = newProperties();
            props.putAll(nodes.getTemplate().getOptions());
            props.putAll(defaults);
            ConnectionInfo connInfo = new ConnectionInfo(cacheKey, nodes, props);

            ClickHouseClientBuilder clientBuilder = ClickHouseClient.builder()
                    .options(ClickHouseDriver.toClientOptions(connInfo.getProperties()))
                    .defaultCredentials(connInfo.getDefaultCredentials());

            final ClickHouseClient client;
            final ClickHouseNode node;
            if (nodes.isSingleNode()) {
                try {
                    node = nodes.apply(nodes.getNodeSelector());
                } catch (Exception e) {
                    throw SqlExceptionUtils.clientError("Failed to get single-node", e);
                }
                client = clientBuilder.nodeSelector(ClickHouseNodeSelector.of(node.getProtocol())).build();
                clientRequest = client.connect(node);
            } else {
                log.debug("Selecting node from: %s", nodes);
                client = clientBuilder.build(); // use dummy client
                clientRequest = client.connect(nodes);
                try {
                    node = clientRequest.getServer();
                } catch (Exception e) {
                    throw SqlExceptionUtils.clientError("No healthy node available", e);
                }
            }

        } catch (IllegalArgumentException e) {
            throw SqlExceptionUtils.clientError(e);
        }

        try (ClickHouseResponse response = clientRequest.option(ClickHouseClientOption.ASYNC, false)
                .option(ClickHouseClientOption.COMPRESS, false).option(ClickHouseClientOption.DECOMPRESS, false)
                .option(ClickHouseClientOption.FORMAT, ClickHouseFormat.RowBinaryWithNamesAndTypes)
                .query(SQL_GET_HOST_PORT_IN_CLUSTER).executeAndWait()) {

            String endpoints="";
            boolean hasNodes=false;
            for(ClickHouseRecord record : response.records())
            {
                hasNodes=true;
                endpoints += record.getValue(0).asString();
                endpoints += ":";
                endpoints += record.getValue(1).asString();
                endpoints += ",";
            }
            if(hasNodes) endpoints=endpoints.substring(0,endpoints.length()-1);

            if(!hasNodes) throw SqlExceptionUtils.clientError("No node in cluster or cannot get node info using SQL: "+SQL_GET_HOST_PORT_IN_CLUSTER);

            log.debug("Get direct endpoints: %s", endpoints);

            return newJdbcUrlPrefix+endpoints+newJdbcUrlSuffix;
        } catch (Exception e) {
            SQLException sqlExp = SqlExceptionUtils.handle(e);
            throw sqlExp;
        }
    }

    public static ConnectionInfo parse(String jdbcUrl, Properties defaults) throws SQLException {
        if (defaults == null) {
            defaults = new Properties();
        }

        Boolean isTransToMultiDirectConn = Boolean.parseBoolean(defaults.getProperty(ClickHouseClientOption.TRANS_TO_MULTI_DIRECT_CONN.getKey(), Boolean.FALSE.toString()));

        if (ClickHouseChecker.isNullOrBlank(jdbcUrl)) {
            throw SqlExceptionUtils.clientError("Non-blank JDBC URL is required");
        }

        if (jdbcUrl.startsWith(JDBC_CLICKHOUSE_PREFIX)) {
            jdbcUrl = jdbcUrl.substring(JDBC_CLICKHOUSE_PREFIX.length());
        } else if (jdbcUrl.startsWith(JDBC_ABBREVIATION_PREFIX)) {
            jdbcUrl = jdbcUrl.substring(JDBC_ABBREVIATION_PREFIX.length());
        } else {
            throw SqlExceptionUtils.clientError(
                    new URISyntaxException(jdbcUrl, ClickHouseUtils.format("'%s' or '%s' prefix is mandatory",
                            JDBC_CLICKHOUSE_PREFIX, JDBC_ABBREVIATION_PREFIX)));
        }

        int index = jdbcUrl.indexOf("//");
        if (index == -1) {
            throw SqlExceptionUtils
                    .clientError(new URISyntaxException(jdbcUrl, "Missing '//' from the given JDBC URL"));
        } else if (index == 0) {
            jdbcUrl = "http:" + jdbcUrl;
        }

        try {
            String cacheKey = ClickHouseNodes.buildCacheKey(jdbcUrl, defaults);
            ClickHouseNodes nodes = ClickHouseNodes.of(cacheKey, jdbcUrl, defaults);

            //generate new cacheKey and nodes when transform to multiple direct connections
            if(isTransToMultiDirectConn.booleanValue())
            {
                //customerzied internal sql
                SQL_GET_HOST_PORT_IN_CLUSTER=defaults.getProperty(ClickHouseClientOption.TRANS_TO_MULTI_DIRECT_CONN_INTERNAL_SQL.getKey(), SQL_GET_HOST_PORT_IN_CLUSTER);
                log.info("Given internal SQL: %s", SQL_GET_HOST_PORT_IN_CLUSTER);

                if(nodes.getNodes().size()>1)
                {
                    throw SqlExceptionUtils.clientError("Must be only one endpoint when trans_to_multi_direct_conn is true.");
                }
                String directConns=getNewJDBCUrl(jdbcUrl, defaults);
                log.info("Get new jdbcUrl for direct endpoints: %s", directConns);
                cacheKey = ClickHouseNodes.buildCacheKey(directConns, defaults);
                nodes = ClickHouseNodes.of(cacheKey, directConns, defaults);
            }

            Properties props = newProperties();
            props.putAll(nodes.getTemplate().getOptions());
            props.putAll(defaults);
            return new ConnectionInfo(cacheKey, nodes, props);
        } catch (IllegalArgumentException e) {
            throw SqlExceptionUtils.clientError(e);
        }
    }

    private ClickHouseJdbcUrlParser() {
    }
}
