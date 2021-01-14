/* Generated by camel build tools - do NOT edit this file! */
package org.apache.camel.component.google.calendar.stream;

import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.camel.spi.EndpointUriFactory;

/**
 * Generated by camel build tools - do NOT edit this file!
 */
public class GoogleCalendarStreamEndpointUriFactory extends org.apache.camel.support.component.EndpointUriFactorySupport implements EndpointUriFactory {

    private static final String BASE = ":index";

    private static final Set<String> PROPERTY_NAMES;
    private static final Set<String> SECRET_PROPERTY_NAMES;
    static {
<<<<<<< HEAD
        Set<String> props = new HashSet<>(41);
=======
        Set<String> props = new HashSet<>(43);
>>>>>>> dca8b31... CAMEL-15961: Extend configuration with 'syncFlow' parameter
        props.add("backoffMultiplier");
        props.add("destination");
        props.add("initialDelay");
        props.add("consumeFromNow");
        props.add("content");
        props.add("setting");
        props.add("scheduler");
        props.add("emailAddress");
        props.add("bridgeErrorHandler");
        props.add("useFixedDelay");
        props.add("runLoggingLevel");
        props.add("maxResults");
        props.add("backoffErrorThreshold");
        props.add("greedy");
        props.add("clientSecret");
        props.add("text");
        props.add("scheduledExecutorService");
        props.add("ruleId");
        props.add("applicationName");
        props.add("repeatCount");
        props.add("timeUnit");
        props.add("eventId");
        props.add("p12FileName");
        props.add("clientId");
        props.add("considerLastUpdate");
        props.add("query");
        props.add("sendEmptyMessageWhenIdle");
        props.add("schedulerProperties");
        props.add("exchangePattern");
        props.add("index");
        props.add("syncFlow");
        props.add("accessToken");
        props.add("backoffIdleThreshold");
        props.add("contentChannel");
        props.add("delay");
        props.add("calendarId");
        props.add("pollStrategy");
        props.add("startScheduler");
        props.add("scopes");
        props.add("exceptionHandler");
        props.add("user");
        props.add("refreshToken");
        PROPERTY_NAMES = Collections.unmodifiableSet(props);
        Set<String> secretProps = new HashSet<>(6);
        secretProps.add("emailAddress");
        secretProps.add("p12FileName");
        secretProps.add("clientSecret");
        secretProps.add("accessToken");
        secretProps.add("user");
        secretProps.add("refreshToken");
        SECRET_PROPERTY_NAMES = Collections.unmodifiableSet(secretProps);
    }

    @Override
    public boolean isEnabled(String scheme) {
        return "google-calendar-stream".equals(scheme);
    }

    @Override
    public String buildUri(String scheme, Map<String, Object> properties, boolean encode) throws URISyntaxException {
        String syntax = scheme + BASE;
        String uri = syntax;

        Map<String, Object> copy = new HashMap<>(properties);

        uri = buildPathParameter(syntax, uri, "index", null, true, copy);
        uri = buildQueryParameters(uri, copy, encode);
        return uri;
    }

    @Override
    public Set<String> propertyNames() {
        return PROPERTY_NAMES;
    }

    @Override
    public Set<String> secretPropertyNames() {
        return SECRET_PROPERTY_NAMES;
    }

    @Override
    public boolean isLenientProperties() {
        return false;
    }
}

