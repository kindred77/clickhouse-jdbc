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
    public static final String TRANS_TO_MULTI_TCP_DIRECT_CONN = "trans_to_multi_tcp_direct_conn";

    static Properties newProperties() {
        Properties props = new Properties();
        props.setProperty(ClickHouseClientOption.ASYNC.getKey(), Boolean.FALSE.toString());
        props.setProperty(ClickHouseClientOption.FORMAT.getKey(), ClickHouseFormat.RowBinaryWithNamesAndTypes.name());
        return props;
    }

    public static final String SQL_GET_HOST_PORT_IN_CLUSTER = "select distinct host_address,http_port from system.clusters";
    private static String getNewJDBCUrl(String jdbcUrl, Properties defaults) throws SQLException
    {
        System.out.println("input jdbcurl is: "+jdbcUrl);
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
        System.out.println("newJdbcUrlSuffix is: "+newJdbcUrlSuffix);
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
                System.out.println("Selecting node from: "+nodes);
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

            if(!hasNodes) throw SqlExceptionUtils.clientError("No node in cluster or cannot get node info!");

            System.out.println("Get tcp direct endpoints: "+endpoints);

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

        Boolean isTransToMultiTcpDirectConn = Boolean.parseBoolean(defaults.getProperty(ClickHouseClientOption.TRANS_TO_MULTI_TCP_DIRECT_CONN.getKey(), Boolean.FALSE.toString()));

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

            //generate new cacheKey and nodes when transform to multiple tcp direct connections
            if(isTransToMultiTcpDirectConn.booleanValue())
            {
                System.out.println("begin to transform to multiple tcp direct connections.");
                if(nodes.getNodes().size()>1)
                {
                    throw SqlExceptionUtils.clientError("Must be only one endpoint when trans_to_multi_tcp_direct_conn is true.");
                }
                String directTcpConns=getNewJDBCUrl(jdbcUrl, defaults);
                System.out.println("Get new jdbcUrl for tcp direct endpoints: "+directTcpConns);
                cacheKey = ClickHouseNodes.buildCacheKey(directTcpConns, defaults);
                nodes = ClickHouseNodes.of(cacheKey, directTcpConns, defaults);
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
