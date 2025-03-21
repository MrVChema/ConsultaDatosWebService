package com.data.GrupoCuatroS.config;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.List;

// Sample to query by using a user credential
public class BigQueryConfig {

  private static final File DATA_STORE_DIR =
      new File(BigQueryConfig.class.getResource("/").getPath(), "credentials");
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  // i.e redirect_uri http://localhost:61984/Callback
  private static final int LOCAL_RECEIVER_PORT = 61984;

  public static void runAuthUserQuery() {
    // TODO(developer): Replace these variables before running the sample.
    /**
     * Download your OAuth2 configuration from the Google Developers Console API Credentials page.
     * https://console.cloud.google.com/apis/credentials
     */
    Path credentialsPath = Paths.get("path/to/your/client_secret.json");
    List<String> scopes = ImmutableList.of("https://www.googleapis.com/auth/bigquery");
    String query =
        "SELECT name, SUM(number) as total"
            + "  FROM `bigquery-public-data.usa_names.usa_1910_current`"
            + "  WHERE name = 'William'"
            + "  GROUP BY name;";
    authUserQuery(credentialsPath, scopes, query);
  }

  public static void authUserQuery(
      Path credentialsPath, List<String> selectedScopes, String query) {
    // Reading credentials file
    try (InputStream inputStream = Files.newInputStream(credentialsPath)) {

      // Load client_secret.json file
      GoogleClientSecrets clientSecrets =
          GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(inputStream));
      String clientId = clientSecrets.getDetails().getClientId();
      String clientSecret = clientSecrets.getDetails().getClientSecret();

      // Generate the url that will be used for the consent dialog.
      GoogleAuthorizationCodeFlow flow =
          new GoogleAuthorizationCodeFlow.Builder(
                  GoogleNetHttpTransport.newTrustedTransport(),
                  JSON_FACTORY,
                  clientSecrets,
                  selectedScopes)
              .setDataStoreFactory(new FileDataStoreFactory(DATA_STORE_DIR))
              .setAccessType("offline")
              .setApprovalPrompt("auto")
              .build();

      // Exchange an authorization code for  refresh token
      LocalServerReceiver receiver =
          new LocalServerReceiver.Builder().setPort(LOCAL_RECEIVER_PORT).build();
      Credential credential = new AuthorizationCodeInstalledApp(flow, receiver).authorize("user");

      // OAuth2 Credentials representing a user's identity and consent
      GoogleCredentials credentials =
          UserCredentials.newBuilder()
              .setClientId(clientId)
              .setClientSecret(clientSecret)
              .setRefreshToken(credential.getRefreshToken())
              .build();

      // Initialize client that will be used to send requests. This client only needs to be created
      // once, and can be reused for multiple requests.
      BigQuery bigquery =
          BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();

      QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

      TableResult results = bigquery.query(queryConfig);

      results
          .iterateAll()
          .forEach(row -> row.forEach(val -> System.out.printf("%s,", val.toString())));

      System.out.println("Query performed successfully.");

    } catch (BigQueryException | IOException | GeneralSecurityException | InterruptedException ex) {
      System.out.println("Query not performed \n" + ex.toString());
    }
  }
}
/*
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;

@Configuration
public class BigQueryConfig {

    @Bean
    public BigQuery bigQuery() throws IOException {
        
    	InputStream credentialsStream = getClass()
                .getClassLoader()
                .getResourceAsStream("credentials/bigquery-credentials.json");

        if (credentialsStream == null) {
            throw new IllegalArgumentException("Archivo de credenciales no encontrado");
        }
    	
    	GoogleCredentials credentials = GoogleCredentials.fromStream(credentialsStream)
    			.createScoped(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
    	
        return BigQueryOptions.newBuilder()
                .setCredentials(credentials)
                .build()
                .getService();
    }
}
*/