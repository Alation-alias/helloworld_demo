package com.alation.api.utils;

//import com.alation.api.models.AlationTable;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.alation.git.github.util.OverridableProperties;
import com.alation.github.model.AlationConfig;
import com.alation.repo.github.GitHubConfig;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.*;

public class ApiUtils {

	private static Logger logger = Logger.getLogger(ApiUtils.class);

	private AlationConfig alationConfig = null;

	private static final String RETRY_COUNT = "request.retry.count";
	public static final String TOKEN = "token";

	public ApiUtils(AlationConfig conf) {
		this.setAlationConfig(conf);
	}

	public static HttpResponse doPOST(List<String> entity, String partialURLString) throws Exception {
		StringBuffer buffer = new StringBuffer();
		for (Iterator<String> iterator = entity.iterator(); iterator.hasNext();) {
			buffer.append(iterator.next());
			buffer.append("\n"); // Add a new line indicating new record (Alation needs it)
		}

		// Sanity
		logger.info(buffer);

		// Now push it!
		return doPOST(new StringEntity(buffer.toString()), partialURLString);
	}

	public static HttpResponse doPOST(StringEntity entity, String partialURLString) throws Exception {
		logger.info(
				"STARTS : entity.getContentLength=" + entity.getContentLength() + ", partialURL=" + partialURLString);

		HttpResponse response = null;
		HttpClient httpClient = HttpClientBuilder.create().build();
		HttpPost httpPost = new HttpPost(PropHelper.getHelper().getBaseURL() + partialURLString);
		httpPost.setHeader("token", PropHelper.getHelper().getToken());
		httpPost.setEntity(entity);
		httpPost.setHeader("Content-Type", "application/json");
		response = httpClient.execute(httpPost);

		logger.info("ENDS : entity.getContentLength=" + entity.getContentLength() + ", response=" + response);

		return response;
	}

	public static HttpResponse doPOST(String baseUrl, StringEntity entity, String partialURLString) throws Exception {
		HttpResponse response = null;
		HttpClient httpClient = HttpClientBuilder.create().build();
		HttpPost httpPost = new HttpPost(baseUrl + partialURLString);
		httpPost.setHeader("token", PropHelper.getHelper().getToken());
		if (entity != null) {
			httpPost.setEntity(entity);
		}
		httpPost.setHeader("Content-Type", "application/json");
		response = httpClient.execute(httpPost);

		logger.info("ENDS : entity.getContentLength=" + entity + ", response=" + response);

		return response;
	}

	public static HttpResponse doPost(String url, StringEntity entity, Map<String, String> headers) throws Exception {
		HttpResponse response = null;
		HttpClient httpClient = HttpClientBuilder.create().build();
		HttpPost httpPost = new HttpPost(url);
		for (String header : headers.keySet()) {
			httpPost.setHeader(header, headers.get(header));
		}
		if (entity != null) {
			httpPost.setEntity(entity);
		}
		response = httpClient.execute(httpPost);
		return response;
	}

	/**
	 * This method is specific to post files to server using curl in a specific
	 * format that the Java runtime curl accepts
	 *
	 * @param fileEntities     list of file names
	 * @param partialURLString url to append to base
	 * @throws Exception
	 */
	public static void doPOSTFiles(List<String> fileEntities, String partialURLString, boolean removeOld)
			throws Exception {
		logger.info("STARTS : filesLength=" + fileEntities.size() + ", partialURL=" + partialURLString);

		// Use Multipart entity to send in all the files as a single request
		MultipartEntityBuilder entity = MultipartEntityBuilder.create();
		HttpResponse response = null;

		// Loop through all the files and Build a multipart request
		for (int index = 0; index < fileEntities.size(); index++) {
			logger.info("Adding file to request object: " + fileEntities.get(index));
			entity.addPart("file", new FileBody(new File(fileEntities.get(index))));
		}

		HttpEntity httpEntity = entity.build();

		try {

			HttpClient httpClient = HttpClientBuilder.create().build();
			String postString = PropHelper.getHelper().getBaseURL() + partialURLString + "?remove_old=" + removeOld;
			logger.info("Posting to: " + postString);
			logger.info("Token: " + PropHelper.getHelper().getToken());
			HttpPost httpPost = new HttpPost(postString);
			httpPost.setHeader("token", PropHelper.getHelper().getToken());
			httpPost.setEntity(httpEntity);

			response = httpClient.execute(httpPost);
			logger.info("Response from server");
			logger.info(new JSONParser().parse(ApiUtils.convert(response.getEntity().getContent())));

		} catch (IOException e1) {
			logger.error("Error in posting the data to server");
			logger.error(e1.getMessage(), e1);
		}
	}

	public static HttpResponse doGET(String partialURLString) throws Exception {
		HttpResponse response = null;
		HttpClient httpClient = HttpClientBuilder.create().build();
		HttpGet httpGet = new HttpGet(PropHelper.getHelper().getBaseURL() + partialURLString);
		httpGet.setHeader("token", PropHelper.getHelper().getToken());
		response = httpClient.execute(httpGet);
		return response;
	}

	// Generic method when we want to pass baseUrl, token and partial url
	public static HttpResponse doGETWithoutSSL(String baseUrl, String token, String partialURLString) throws Exception {
		HttpResponse response = null;

		int retryCount = getRetryCount();

		DefaultHttpRequestRetryHandler retryHandler = new DefaultHttpRequestRetryHandler(retryCount, true);

		// Setting default retry handler
		/*
		 * HttpClient httpClient = HttpClients.custom() .setRetryHandler(retryHandler)
		 * .build();
		 */
		HttpClient httpClient = HttpClients.custom().setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
				.setSSLContext(new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy() {
					public boolean isTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
						return true;
					}
				}).build()).build();
		// Building Get request and setting headers
		System.out.println("baseUrl + partialURLString::: " + baseUrl + partialURLString);
		HttpGet httpGet = new HttpGet(baseUrl + partialURLString);
		// httpGet.setHeader(HttpHeaders.AUTHORIZATION, token);
		response = httpClient.execute(httpGet);
		return response;
	}

	// Generic method when we want to pass baseUrl, token and partial url
	public static HttpResponse doGET(String baseUrl, String token, String partialURLString) throws Exception {
		HttpResponse response = null;

		int retryCount = getRetryCount();

		DefaultHttpRequestRetryHandler retryHandler = new DefaultHttpRequestRetryHandler(retryCount, true);

		// Setting default retry handler
		HttpClient httpClient = HttpClients.custom().setRetryHandler(retryHandler).build();
		// Building Get request and setting headers
		HttpGet httpGet = new HttpGet(baseUrl + partialURLString);
		httpGet.setHeader(HttpHeaders.AUTHORIZATION, token);
		response = httpClient.execute(httpGet);
		return response;
	}

	public static String convert(InputStream inputStream) throws IOException {
		try (Scanner scanner = new Scanner(inputStream)) {
			if (scanner.useDelimiter("\\A").hasNext()) {
				return scanner.useDelimiter("\\A").next();
			}
			return "";
		}
	}

	public static void print(HttpResponse response) {
		logger.info("Status : " + response.getStatusLine().getStatusCode() + ", Message: "
				+ response.getStatusLine().getReasonPhrase());
	}

	/**
	 * Get retry count from property file
	 *
	 * @return int retry value
	 */
	private static int getRetryCount() {
		int retryCount = 0;
		try {
			if (PropHelper.getHelper().getProperties().get(RETRY_COUNT) != null) {
				retryCount = Integer.parseInt(PropHelper.getHelper().getProperties().get(RETRY_COUNT).toString());
				logger.info("Setting retry count to: " + retryCount);
				return retryCount;
			}
		} catch (Exception ex) {
			
			logger.error(ex.getMessage(), ex);
		}
		
		return retryCount;
	}

	/**
	 * Submit the Tables and columns to alation
	 *
	 * @param jsonString
	 * @param dataSourceId
	 */
	public void submitToAlation(String jsonString, String dataSourceId) {
		try {
			logger.info("submitToAlation() Posting to alation");
			doPOST(new StringEntity(jsonString, ContentType.APPLICATION_JSON),
					"/api/v1/bulk_metadata/extraction/" + dataSourceId);
			logger.info("submitToAlation() Posting to alation Completed");
		} catch (Exception e) {
			logger.error("Error posting data to source source");
			logger.error(e.getMessage(), e);
		}
	}

	/**
	 * Post lineage to Alation
	 *
	 * @param lineages
	 * @throws Exception
	 */

	// code
	public static HttpResponse doVFSPOST(StringEntity entity, String partialURLString) throws Exception {

		HttpResponse response = null;
		CloseableHttpClient httpClient;
		// Check whether the IGNORE_SSL property is true, if true add self signed SSL to
		// the request
		if (OverridableProperties.IGNORE_SSL) {
			httpClient = HttpClients.custom().setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
					.setSSLContext(new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy() {
						public boolean isTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
							return true;
						}
					}).build()).build();
		} else {
			HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
			httpClient = httpClientBuilder.build();
		}
		HttpPost httpPost = new HttpPost(PropHelper.getHelper().getBaseURL() + partialURLString);
		// HttpPost httpPost = new HttpPost(getAlationConfig().getHostName() +
		// partialURLString);
		httpPost.setHeader(TOKEN, GitHubConfig.getAccessToken());
		httpPost.setEntity(entity);
		// httpPost.setHeader(CONTENT_TYPE, CONTENT.JSON.getType());
		response = httpClient.execute(httpPost);

		// print((getAlationConfig().getHostName() + partialURLString), response);
		return response;
	}

	public static JSONArray doGet(String partialUrl, String token) throws Exception {
		// String baseUrl = GITHUB_SOURCE_PREFIX_URL;
		String baseUrl = PropHelper.getHelper().getGitHubbaseURL();
		JSONArray result = new JSONArray();
		HttpResponse response = ApiUtils.doGET(baseUrl, token, partialUrl);

		if (response.getStatusLine().getStatusCode() != 200) {
			logger.fatal("Got an invalid HTTP Response, returning null.");
			logger.error(response);

		} else {
			result = (JSONArray) new JSONParser().parse(ApiUtils.convert(response.getEntity().getContent()));
		}

		return result;
	}

	public static HttpResponse doPOST(StringEntity entity) throws Exception {
		String filesystemid = PropHelper.getHelper().getFilesystemid();
		String baseUrl = PropHelper.getHelper().getBaseURL();
		HttpResponse response = null;
		String partialURL = "api/v1/bulk_metadata/file_upload/";
		HttpClient httpClient = HttpClientBuilder.create().build();
		HttpPost httpPost = new HttpPost(baseUrl + partialURL + filesystemid + "/");
		httpPost.setEntity(entity);
		httpPost.setHeader("token", PropHelper.getHelper().getToken());
		response = httpClient.execute(httpPost);
		logger.info(response);
		return response;
	}

	public AlationConfig getAlationConfig() {
		return alationConfig;
	}

	public void setAlationConfig(AlationConfig alationConfig) {
		this.alationConfig = alationConfig;
	}

	public JSONObject submitVFSToAlation(String url2Post, String fullPayload) throws Exception {
		logger.info("submitToAlation(_STARTS) url=" + url2Post);
		HttpResponse response = doPOST(new StringEntity(fullPayload), url2Post);
		logger.info("submitToAlation(_ENDS) response=" + response);
		return ((JSONObject) new JSONParser().parse(convert(response.getEntity().getContent())));
	}

	
}