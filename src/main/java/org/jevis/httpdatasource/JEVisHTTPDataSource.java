package org.jevis.httpdatasource;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
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
public class JEVisHTTPDataSource implements DataSource {

    private Parser _parser;
    private Importer _importer;
    private List<JEVisObject> _channels;
    private List<Result> _result;

    private JEVisObject _dataSource;
    private HTTPDataSource _httpdatasource;

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
                java.util.logging.Logger.getLogger(JEVisHTTPDataSource.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
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

    @Override
    public List<InputStream> sendSampleRequest(JEVisObject channel) {
        HTTPChannel httpChannel = new HTTPChannel();

        try {
            JEVisClass channelClass = channel.getJEVisClass();
            JEVisType pathType = channelClass.getType(DataCollectorTypes.Channel.HTTPChannel.PATH);
            String path = DatabaseHelper.getObjectAsString(channel, pathType);
            JEVisType readoutType = channelClass.getType(DataCollectorTypes.Channel.HTTPChannel.LAST_READOUT);
            DateTime lastReadout = DatabaseHelper.getObjectAsDate(channel, readoutType, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"));
            httpChannel.setLastReadout(lastReadout);
            httpChannel.setPath(path);
        } catch (JEVisException ex) {
            java.util.logging.Logger.getLogger(JEVisHTTPDataSource.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }

        return _httpdatasource.sendSampleRequest(httpChannel);
    }

    private void initializeAttributes(JEVisObject httpObject) {
        try {
            JEVisClass httpType = httpObject.getDataSource().getJEVisClass(DataCollectorTypes.DataSource.DataServer.HTTP.NAME);
            JEVisType server = httpType.getType(DataCollectorTypes.DataSource.DataServer.HTTP.HOST);
            JEVisType portType = httpType.getType(DataCollectorTypes.DataSource.DataServer.HTTP.PORT);
            JEVisType sslType = httpType.getType(DataCollectorTypes.DataSource.DataServer.HTTP.SSL);
            JEVisType connectionTimeoutType = httpType.getType(DataCollectorTypes.DataSource.DataServer.HTTP.CONNECTION_TIMEOUT);
            JEVisType readTimeoutType = httpType.getType(DataCollectorTypes.DataSource.DataServer.HTTP.READ_TIMEOUT);
            JEVisType userType = httpType.getType(DataCollectorTypes.DataSource.DataServer.HTTP.USER);
            JEVisType passwordType = httpType.getType(DataCollectorTypes.DataSource.DataServer.HTTP.PASSWORD);
            JEVisType timezoneType = httpType.getType(DataCollectorTypes.DataSource.DataServer.HTTP.TIMEZONE);
            JEVisType enableType = httpType.getType(DataCollectorTypes.DataSource.DataServer.HTTP.ENABLE);

            String serverURL = DatabaseHelper.getObjectAsString(httpObject, server);
            Integer port = DatabaseHelper.getObjectAsInteger(httpObject, portType);
            Integer connectionTimeout = DatabaseHelper.getObjectAsInteger(httpObject, connectionTimeoutType);
            Integer readTimeout = DatabaseHelper.getObjectAsInteger(httpObject, readTimeoutType);
            Boolean ssl = DatabaseHelper.getObjectAsBoolean(httpObject, sslType);
            JEVisAttribute userAttr = httpObject.getAttribute(userType);
            String userName = null;
            if (!userAttr.hasSample()) {
                userName = "";
            } else {
                userName = (String) userAttr.getLatestSample().getValue();
            }
            JEVisAttribute passAttr = httpObject.getAttribute(passwordType);
            String password = null;
            if (!passAttr.hasSample()) {
                password = "";
            } else {
                password = (String) passAttr.getLatestSample().getValue();
            }
//            _lastReadout = DatabaseHelper.getObjectAsDate(httpObject, lastReadout, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"));

            _httpdatasource = new HTTPDataSource();
            _httpdatasource.setConnectionTimeout(connectionTimeout);
            _httpdatasource.setPassword(password);
            _httpdatasource.setPort(port);
            _httpdatasource.setReadTimeout(readTimeout);
            _httpdatasource.setServerURL(serverURL);
            _httpdatasource.setSsl(ssl);
            _httpdatasource.setUserName(userName);

        } catch (JEVisException ex) {
            Logger.getLogger(JEVisHTTPDataSource.class.getName()).log(Level.ERROR, null, ex);
        }
    }

    private void initializeChannelObjects(JEVisObject httpObject) {
        try {
            JEVisClass channelDirClass = httpObject.getDataSource().getJEVisClass(DataCollectorTypes.ChannelDirectory.HTTPChannelDirectory.NAME);
            JEVisObject channelDir = httpObject.getChildren(channelDirClass, false).get(0);
            JEVisClass channelClass = httpObject.getDataSource().getJEVisClass(DataCollectorTypes.Channel.HTTPChannel.NAME);
            _channels = channelDir.getChildren(channelClass, false);
        } catch (Exception ex) {
            java.util.logging.Logger.getLogger(JEVisHTTPDataSource.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
    }

}
