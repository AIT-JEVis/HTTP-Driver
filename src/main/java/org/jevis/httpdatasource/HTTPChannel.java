/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jevis.httpdatasource;

import org.joda.time.DateTime;

/**
 *
 * @author broder
 */
public class HTTPChannel {
    private String _path;
    private DateTime _lastReadout;

    public String getPath() {
        return _path;
    }

    public void setPath(String _path) {
        this._path = _path;
    }

    public DateTime getLastReadout() {
        return _lastReadout;
    }

    public void setLastReadout(DateTime _lastReadout) {
        this._lastReadout = _lastReadout;
    }
    
    
}
