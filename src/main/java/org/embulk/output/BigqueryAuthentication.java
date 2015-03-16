package org.embulk.output;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.IllegalFormatException;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.auth.oauth2.CredentialRefreshListener;
import com.google.api.client.auth.oauth2.TokenErrorResponse;
import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.common.collect.ImmutableList;
import java.security.GeneralSecurityException;

import org.embulk.spi.Exec;
import org.slf4j.Logger;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.ProjectList;

public class BigqueryAuthentication
{

    private final Logger log = Exec.getLogger(BigqueryAuthentication.class);
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
        GoogleCredential cred = new GoogleCredential.Builder()
                .setTransport(httpTransport)
                .setJsonFactory(jsonFactory)
                .setServiceAccountId(serviceAccountEmail)
                .setServiceAccountScopes(
                        ImmutableList.of(
                                BigqueryScopes.DEVSTORAGE_READ_WRITE,
                                BigqueryScopes.BIGQUERY
                        )
                )
                .setServiceAccountPrivateKeyFromP12File(new File(p12KeyFilePath))
                .build();
        return cred;
    }

    public Bigquery getBigqueryClient() throws IOException
    {
        Bigquery client = new Bigquery.Builder(httpTransport, jsonFactory, credentials)
                .setHttpRequestInitializer(credentials)
                .setApplicationName(applicationName)
                .build();

        // For throw IOException when authentication is failed.
        long maxResults = 1;
        Bigquery.Projects.List req = client.projects().list().setMaxResults(maxResults);
        ProjectList projectList = req.execute();

        return client;
    }

    public Storage getGcsClient() throws IOException
    {
        Storage client = new Storage.Builder(httpTransport, jsonFactory, credentials)
                .setApplicationName(applicationName)
                .build();

        return client;
    }
}