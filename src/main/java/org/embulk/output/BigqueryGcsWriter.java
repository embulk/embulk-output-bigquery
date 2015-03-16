package org.embulk.output;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collection;
import java.util.Iterator;
import java.util.IllegalFormatException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
//import eu.medsea.mimeutil.MimeType;
//import eu.medsea.mimeutil.MimeUtil;
//import eu.medsea.mimeutil.detector.MimeDetector;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.codec.binary.Base64;
import java.security.GeneralSecurityException;

import org.embulk.spi.Exec;
import org.slf4j.Logger;

import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;

import com.google.api.client.http.InputStreamContent;

public class BigqueryGcsWriter
{

    private final Logger log = Exec.getLogger(BigqueryGcsWriter.class);
    private final String bucket;
    private final String sourceFormat;
    private final boolean isFileCompressed;
    private final boolean deleteFromBucketWhenJobEnd;
    private Storage storageClient;

    public BigqueryGcsWriter(Builder builder) throws IOException, GeneralSecurityException
    {
        this.bucket = builder.bucket;
        this.sourceFormat = builder.sourceFormat.toUpperCase();
        this.isFileCompressed = builder.isFileCompressed;
        this.deleteFromBucketWhenJobEnd = builder.deleteFromBucketWhenJobEnd;

        BigqueryAuthentication auth = new BigqueryAuthentication(builder.serviceAccountEmail, builder.p12KeyFilePath, builder.applicationName);
        this.storageClient = auth.getGcsClient();
    }

    public void uploadFile(String localFilePath, String fileName, Optional<String> remotePath) throws IOException
    {
        FileInputStream stream = null;

        try {
            String path;
            if (remotePath.isPresent()) {
                path = remotePath.get();
            } else {
                path = "";
            }
            String gcsPath = getRemotePath(path, fileName);
            StorageObject objectMetadata = new StorageObject().setName(gcsPath);
            log.info(String.format("Uploading file [%s] to [gs://%s/%s]", localFilePath, bucket, gcsPath));

            File file = new File(localFilePath);
            stream = new FileInputStream(file);
            InputStreamContent content = new InputStreamContent(getContentType(), stream);
            Storage.Objects.Insert insertObject = storageClient.objects().insert(bucket, objectMetadata, content);
            insertObject.setDisableGZipContent(true);

            StorageObject response = insertObject.execute();
            log.info(String.format("Upload completed [%s] to [gs://%s/%s]", localFilePath, bucket, gcsPath));
        } finally {
            stream.close();
        }
    }

    private String getRemotePath(String remotePath, String fileName)
    {
        if (remotePath.isEmpty()) {
            return fileName;
        }
        String[] pathList = StringUtils.split(remotePath, '/');
        String path = StringUtils.join(pathList) + "/";
        if (!path.endsWith("/")) {
            path = path + "/";
        }
        return path + fileName;
    }

    public void deleteFile(String remotePath, String fileName) throws IOException
    {
        String path = getRemotePath(remotePath, fileName);
        storageClient.objects().delete(bucket, path).execute();
        log.info(String.format("Delete remote file [gs://%s/%s]", bucket, path));
    }

    public boolean getDeleteFromBucketWhenJobEnd()
    {
        return this.deleteFromBucketWhenJobEnd;
    }

    private String getContentType()
    {
        if (isFileCompressed) {
            return "application/x-gzip";
        } else {
            if (sourceFormat.equals("NEWLINE_DELIMITED_JSON)")) {
                return "application/json";
            } else {
                return "text/csv";
            }
        }
    }

    /*
    private void registerMimeDetector()
    {
        String mimeDetector = "eu.medsea.mimeutil.detector.MagicMimeMimeDetector";
        MimeDetector registeredMimeDetector = MimeUtil.getMimeDetector(mimeDetector);
        MimeUtil.registerMimeDetector(mimeDetector);
    }

    public String detectMimeType(File file)
    {
        try {
            Collection<?> mimeTypes = MimeUtil.getMimeTypes(file);
            if (!mimeTypes.isEmpty()) {
                Iterator<?> iterator = mimeTypes.iterator();
                MimeType mimeType = (MimeType) iterator.next();
                return mimeType.getMediaType() + "/" + mimeType.getSubType();
            }
        } catch (Exception ex) {
        }
        return "application/octet-stream";
    }
    */

    public static class Builder
    {
        private final String serviceAccountEmail;
        private String p12KeyFilePath;
        private String applicationName;
        private String bucket;
        private String sourceFormat;
        private boolean isFileCompressed;
        private boolean deleteFromBucketWhenJobEnd;
        private boolean enableMd5hashCheck;

        public Builder(String serviceAccountEmail)
        {
            this.serviceAccountEmail = serviceAccountEmail;
        }

        public Builder setP12KeyFilePath(String p12KeyFilePath)
        {
            this.p12KeyFilePath = p12KeyFilePath;
            return this;
        }

        public Builder setApplicationName(String applicationName)
        {
            this.applicationName = applicationName;
            return this;
        }

        public Builder setBucket(String bucket)
        {
            this.bucket = bucket;
            return this;
        }

        public Builder setSourceFormat(String sourceFormat)
        {
            this.sourceFormat = sourceFormat;
            return this;
        }

        public Builder setIsFileCompressed(boolean isFileCompressed)
        {
            this.isFileCompressed = isFileCompressed;
            return this;
        }

        public Builder setDeleteFromBucketWhenJobEnd(boolean deleteFromBucketWhenJobEnd)
        {
            this.deleteFromBucketWhenJobEnd = deleteFromBucketWhenJobEnd;
            return this;
        }

        public BigqueryGcsWriter build() throws IOException, GeneralSecurityException
        {
            return new BigqueryGcsWriter(this);
        }
    }
}