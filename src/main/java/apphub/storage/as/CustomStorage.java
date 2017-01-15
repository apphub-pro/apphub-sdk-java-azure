/*
 * Copyright (C) 2014 Dmitry Kotlyarov.
 * All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package apphub.storage.as;

import apphub.storage.CreateStorageException;
import apphub.storage.Storage;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;

/**
 * @author Dmitry Kotlyarov
 * @since 1.0
 */
public class CustomStorage extends Storage {
    protected final CloudBlobClient client;
    protected final CloudBlobContainer container;

    public CustomStorage(URL url) {
        this(url.getHost(), url.getPath(), createClient(url));
    }

    public CustomStorage(String bucket, String prefix, CloudBlobClient client) {
        super("as", bucket, prefix);

        try {
            this.client = client;
            this.container = client.getContainerReference(bucket);
        } catch (Exception e) {
            throw new CreateStorageException(getUrl(), e);
        }
    }

    public CloudBlobClient getClient() {
        return client;
    }

    public CloudBlobContainer getContainer() {
        return container;
    }

    @Override
    public String getHttpsUrl() {
        return null;
    }

    @Override
    public Storage createSubstorage(String prefix) {
        return null;
    }

    @Override
    public Iterable<String> find(String prefix, String filter) {
        return null;
    }

    @Override
    public Iterable<String> list(String prefix, String filter) {
        return null;
    }

    @Override
    public void getMeta(String key, Map<String, String> meta) {

    }

    @Override
    public byte[] get(String key, Map<String, String> meta) {
        return new byte[0];
    }

    @Override
    public InputStream getStream(String key, Map<String, String> meta) {
        return null;
    }

    @Override
    public long put(String key, byte[] data, Map<String, String> meta) {
        return 0;
    }

    @Override
    public long upload(String key, InputStream input, long size, Map<String, String> meta) {
        return 0;
    }

    @Override
    public void remove(String key) {

    }

    @Override
    public String getUrl(String key) {
        return null;
    }

    @Override
    public String getHttpsUrl(String key) {
        return null;
    }

    protected static CloudBlobClient createClient(URL url) {
        CloudBlobClient client;
        String userInfo = url.getUserInfo();
        if (userInfo != null) {
            String[] creds = userInfo.split(":");
            if (creds.length == 2) {
                try {
                    client = new CloudStorageAccount(new StorageCredentialsAccountAndKey(creds[0], creds[1]), true).createCloudBlobClient();
                } catch (URISyntaxException e) {
                    throw new CreateStorageException(url.toString(), e);
                }
            } else {
                throw new CreateStorageException(url.toString(), "Credentials for Azure storage must be in form of ACCOUNT:KEY");
            }
        } else {
            throw new CreateStorageException(url.toString(), "Credentials for Azure storage must be specified");
        }
        return client;
    }
}
