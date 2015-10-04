package org.jevis.httpdatasource;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jevis.api.JEVisAttribute;
import org.jevis.api.JEVisClass;
import org.jevis.api.JEVisException;
import org.jevis.api.JEVisObject;
import org.jevis.api.JEVisType;
import org.jevis.commons.DatabaseHelper;
import org.jevis.commons.driver.DataSourceHelper;
import org.jevis.commons.driver.Importer;
import org.jevis.commons.driver.ImporterFactory;
import org.jevis.commons.driver.DataCollectorTypes;
import org.jevis.commons.driver.Parser;
import org.jevis.commons.driver.ParserFactory;
import org.jevis.commons.driver.Result;
import org.jevis.commons.driver.DataSource;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

/**
 *
 * @author bf
 */
public class HTTPDataSource implements DataSource {

    // interfaces
    interface HTTP extends DataCollectorTypes.DataSource.DataServer {

        public final static String NAME = "HTTP Server";
        public final static String PASSWORD = "Password";
        public final static String SSL = "SSL";
        public final static String USER = "User";
    }
    
    interface HTTPChannelDirectory extends DataCollectorTypes.ChannelDirectory {

        public final static String NAME = "HTTP Channel Directory";
    }
    
    interface HTTPChannel extends DataCollectorTypes.Channel {

        public final static String NAME = "HTTP Channel";
        public final static String PATH = "Path";
    }

    // member variables
    private Long _id;
    private String _name;
    private String _serverURL;
    private Integer _port;
    private Integer _connectionTimeout;
    private Integer _readTimeout;
    private String _userName;
    private String _password;
    private Boolean _ssl = false;
    private String _timezone;
    private Boolean _enabled;

    private Parser _parser;
    private Importer _importer;
    private List<JEVisObject> _channels;
    private List<Result> _result;

    private JEVisObject _dataSource;

    @Override
    public void parse(List<InputStream> input) {
        _parser.parse(input);
        _result = _parser.getResult();
    }

    @Override
    public void run() {

        for (JEVisObject channel : _channels) {

            try {
                _result = new ArrayList<Result>();
                JEVisClass parserJevisClass = channel.getDataSource().getJEVisClass(DataCollectorTypes.Parser.NAME);
                JEVisObject parser = channel.getChildren(parserJevisClass, true).get(0);

                _parser = ParserFactory.getParser(parser);
                _parser.initialize(parser);

                List<InputStream> input = this.sendSampleRequest(channel);

                this.parse(input);

                if (!_result.isEmpty()) {

                    this.importResult();

                    DataSourceHelper.setLastReadout(channel, _importer.getLatestDatapoint());
                }
            } catch (Exception ex) {
                java.util.logging.Logger.getLogger(HTTPDataSource.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
            }
        }
    }

    @Override
    public void importResult() {
        _importer.importResult(_result);
    }

    @Override
    public void initialize(JEVisObject httpObject) {
        _dataSource = httpObject;
        initializeAttributes(httpObject);
        initializeChannelObjects(httpObject);

        _importer = ImporterFactory.getImporter(_dataSource);
        if (_importer != null) {
            _importer.initialize(_dataSource);
        }

    }

    /**
     * komplett überarbeiten!!!!!
     *
     * @param channel
     * @return
     */
    @Override
    public List<InputStream> sendSampleRequest(JEVisObject channel) {
        List<InputStream> answer = new ArrayList<InputStream>();
        try {
            JEVisClass channelClass = channel.getJEVisClass();
            JEVisType pathType = channelClass.getType(HTTPChannel.PATH);
            String path = DatabaseHelper.getObjectAsString(channel, pathType);
            JEVisType readoutType = channelClass.getType(HTTPChannel.LAST_READOUT);
            DateTime lastReadout = DatabaseHelper.getObjectAsDate(channel, readoutType, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"));
            if (path.startsWith("/")) {
                path = path.substring(1, path.length());
            }

            URL requestUrl;
            if (_userName == null || _password == null || _userName.equals("") || _password.equals("")) {

                path = DataSourceHelper.replaceDateFromUntil(lastReadout, new DateTime(), path);
                HttpURLConnection request = null;
                if (!_serverURL.contains("://")) {
                    _serverURL = "http://" + _serverURL;
                }

                if (_ssl) {
                    _serverURL = _serverURL.replace("http", "https");
                }

                if (_port != null) {
                    requestUrl = new URL(_serverURL + ":" + _port + "/" + path);
                } else {
                    requestUrl = new URL(_serverURL + "/" + path);
                }
                if (_ssl) {
                    DataSourceHelper.doTrustToCertificates();
                }
                Logger.getLogger(HTTPDataSource.class.getName()).log(Level.INFO, "Connection URL: " + requestUrl);
                request = (HttpURLConnection) requestUrl.openConnection();

//                    if (_connectionTimeout == null) {
                _connectionTimeout = _connectionTimeout * 1000;
//                    }
                //                System.out.println("Connect timeout: " + _connectionTimeout.intValue() / 1000 + "s");
                request.setConnectTimeout(_connectionTimeout.intValue());

//                    if (_readTimeout == null) {
                _readTimeout = _readTimeout * 1000;
//                    }
                //                System.out.println("read timeout: " + _readTimeout.intValue() / 1000 + "s");
                request.setReadTimeout(_readTimeout.intValue());
                answer.add(request.getInputStream());
            } else {
                DefaultHttpClient _httpClient;
                HttpHost _targetHost;
                HttpGet _httpGet;
                BasicHttpContext _localContext = new BasicHttpContext();
                _httpClient = new DefaultHttpClient();

                path = DataSourceHelper.replaceDateFromUntil(lastReadout, new DateTime(), path);
                if (_ssl) {
                    DataSourceHelper.doTrustToCertificates();
                    _targetHost = new HttpHost(_serverURL, ((int) (long) _port), "https");
                } else {
                    _targetHost = new HttpHost(_serverURL, ((int) (long) _port), "http");
                }
                /*
                 * set the sope for the authentification
                 */
                _httpClient.getCredentialsProvider().setCredentials(
                        new AuthScope(_targetHost.getHostName(), _targetHost.getPort()),
                        new UsernamePasswordCredentials(_userName, _password));

                // Create AuthCache instance
                AuthCache authCache = new BasicAuthCache();

                //set Authenticication scheme
                BasicScheme basicAuth = new BasicScheme();
                authCache.put(_targetHost, basicAuth);

                path = DataSourceHelper.replaceDateFromUntil(lastReadout, new DateTime(), path);

                _httpGet = new HttpGet(path);
                //TODO: Connection timeouts and error handling

                HttpResponse oResponse = _httpClient.execute(_targetHost, _httpGet, _localContext);

                HttpEntity oEntity = oResponse.getEntity();
                String oXmlString = EntityUtils.toString(oEntity);
                EntityUtils.consume(oEntity);
                InputStream stream = new ByteArrayInputStream(oXmlString.getBytes("UTF-8"));
                answer.add(stream);

            }
//        List<InputHandler> answerList = new ArrayList<InputHandler>();
//        answerList.add(InputHandlerFactory.getInputConverter(answer));
        } catch (JEVisException ex) {
            Logger.getLogger(HTTPDataSource.class.getName()).log(Level.ERROR, ex.getMessage());
            java.util.logging.Logger.getLogger(HTTPDataSource.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (MalformedURLException ex) {
            Logger.getLogger(HTTPDataSource.class.getName()).log(Level.ERROR, ex.getMessage());
            java.util.logging.Logger.getLogger(HTTPDataSource.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(HTTPDataSource.class.getName()).log(Level.ERROR, ex.getMessage());
            java.util.logging.Logger.getLogger(HTTPDataSource.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        return answer;
    }

    private void initializeAttributes(JEVisObject httpObject) {
        try {
            JEVisClass httpType = httpObject.getDataSource().getJEVisClass(HTTP.NAME);
            JEVisType server = httpType.getType(HTTP.HOST);
            JEVisType port = httpType.getType(HTTP.PORT);
            JEVisType sslType = httpType.getType(HTTP.SSL);
            JEVisType connectionTimeout = httpType.getType(HTTP.CONNECTION_TIMEOUT);
            JEVisType readTimeout = httpType.getType(HTTP.READ_TIMEOUT);
            JEVisType user = httpType.getType(HTTP.USER);
            JEVisType password = httpType.getType(HTTP.PASSWORD);
            JEVisType timezoneType = httpType.getType(HTTP.TIMEZONE);
            JEVisType enableType = httpType.getType(HTTP.ENABLE);

            _id = httpObject.getID();
            _name = httpObject.getName();
            _serverURL = DatabaseHelper.getObjectAsString(httpObject, server);
            _port = DatabaseHelper.getObjectAsInteger(httpObject, port);
            _connectionTimeout = DatabaseHelper.getObjectAsInteger(httpObject, connectionTimeout);
            _readTimeout = DatabaseHelper.getObjectAsInteger(httpObject, readTimeout);
            _ssl = DatabaseHelper.getObjectAsBoolean(httpObject, sslType);
            JEVisAttribute userAttr = httpObject.getAttribute(user);
            if (!userAttr.hasSample()) {
                _userName = "";
            } else {
                _userName = (String) userAttr.getLatestSample().getValue();
            }
            JEVisAttribute passAttr = httpObject.getAttribute(password);
            if (!passAttr.hasSample()) {
                _password = "";
            } else {
                _password = (String) passAttr.getLatestSample().getValue();
            }
//            _lastReadout = DatabaseHelper.getObjectAsDate(httpObject, lastReadout, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"));
            _timezone = DatabaseHelper.getObjectAsString(httpObject, timezoneType);
            _enabled = DatabaseHelper.getObjectAsBoolean(httpObject, enableType);
        } catch (JEVisException ex) {
            Logger.getLogger(HTTPDataSource.class.getName()).log(Level.ERROR, null, ex);
        }
    }

    private void initializeChannelObjects(JEVisObject httpObject) {
        try {
            JEVisClass channelDirClass = httpObject.getDataSource().getJEVisClass(HTTPChannelDirectory.NAME);
            JEVisObject channelDir = httpObject.getChildren(channelDirClass, false).get(0);
            JEVisClass channelClass = httpObject.getDataSource().getJEVisClass(HTTPChannel.NAME);
            _channels = channelDir.getChildren(channelClass, false);
        } catch (Exception ex) {
            java.util.logging.Logger.getLogger(HTTPDataSource.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
    }

}
