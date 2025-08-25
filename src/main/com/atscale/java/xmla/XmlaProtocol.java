package com.atscale.java.xmla;
import io.gatling.javaapi.http.*;
import static io.gatling.javaapi.http.HttpDsl.*;
import com.atscale.java.utils.PropertiesFileReader;

@SuppressWarnings("unused")
public class XmlaProtocol {

    public static HttpProtocolBuilder forXmla(String model) {
        String url = PropertiesFileReader.getAtScaleXmlaConnection(model);
        return http.baseUrl(url)
                .contentTypeHeader("text/xml; charset=UTF-8")
                .acceptHeader("text/xml");
    }
}
