package org.embulk.output;

import java.io.File;
import java.io.IOException;
import com.google.common.collect.ImmutableList;
import java.security.GeneralSecurityException;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.ProjectList;

public class BigqueryAuthentication
{
    private final String serviceAccountEmail;
    private final String p12KeyFilePath;
    private final String applicationName;
    private final HttpTransport httpTransport;
    private final JsonFactory jsonFactory;
    private final GoogleCredential credentials;

    public BigqueryAuthentication(String serviceAccountEmail, String p12KeyFilePath, String applicationName) throws IOException, GeneralSecurityException
    {
        this.serviceAccountEmail = serviceAccountEmail;
        this.p12KeyFilePath = p12KeyFilePath;
        this.applicationName = applicationName;

        this.httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        this.jsonFactory = new JacksonFactory();
        this.credentials = getCredentialProvider();
    }

    /**
     * @see https://developers.google.com/accounts/docs/OAuth2ServiceAccount#authorizingrequests
     */
    private GoogleCredential getCredentialProvider() throws IOException, GeneralSecurityException
    {
        // @see https://cloud.google.com/compute/docs/api/how-tos/authorization
        // @see https://developers.google.com/resources/api-libraries/documentation/storage/v1/java/latest/com/google/api/services/storage/STORAGE_SCOPE.html
        // @see https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/java/latest/com/google/api/services/bigquery/BigqueryScopes.html
        return new GoogleCredential.Builder()
                .setTransport(httpTransport)
                .setJsonFactory(jsonFactory)
                .setServiceAccountId(serviceAccountEmail)
                .setServiceAccountScopes(
                        ImmutableList.of(
                                BigqueryScopes.BIGQUERY
                        )
                )
                .setServiceAccountPrivateKeyFromP12File(new File(p12KeyFilePath))
                .build();
    }

    public Bigquery getBigqueryClient() throws IOException
    {
        Bigquery client = new Bigquery.Builder(httpTransport, jsonFactory, credentials)
                .setHttpRequestInitializer(credentials)
                .setApplicationName(applicationName)
                .build();

        // For throw IOException when authentication is fail.
        long maxResults = 1;
        ProjectList projectList = client.projects().list().setMaxResults(maxResults).execute();

        return client;
    }
}