/****************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one   *
 * or more contributor license agreements.  See the NOTICE file *
 * distributed with this work for additional information        *
 * regarding copyright ownership.  The ASF licenses this file   *
 * to you under the Apache License, Version 2.0 (the            *
 * "License"); you may not use this file except in compliance   *
 * with the License.  You may obtain a copy of the License at   *
 *                                                              *
 *   http://www.apache.org/licenses/LICENSE-2.0                 *
 *                                                              *
 * Unless required by applicable law or agreed to in writing,   *
 * software distributed under the License is distributed on an  *
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY       *
 * KIND, either express or implied.  See the License for the    *
 * specific language governing permissions and limitations      *
 * under the License.                                           *
 ****************************************************************/

package org.apache.james.blob.objectstorage;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import javax.annotation.PreDestroy;

import org.apache.commons.io.IOUtils;
import org.apache.james.blob.api.BlobId;
import org.apache.james.blob.api.BucketName;
import org.apache.james.blob.api.DumbBlobStore;
import org.apache.james.blob.api.IOObjectStoreException;
import org.apache.james.blob.api.ObjectNotFoundException;
import org.apache.james.blob.api.ObjectStoreException;
import org.apache.james.blob.objectstorage.aws.AwsS3AuthConfiguration;
import org.apache.james.blob.objectstorage.aws.AwsS3ObjectStorage;
import org.apache.james.blob.objectstorage.swift.SwiftKeystone2ObjectStorage;
import org.apache.james.blob.objectstorage.swift.SwiftKeystone3ObjectStorage;
import org.apache.james.blob.objectstorage.swift.SwiftTempAuthObjectStorage;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.StorageType;
import org.jclouds.http.HttpResponseException;

import com.github.fge.lambdas.Throwing;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteSource;
import com.google.common.io.FileBackedOutputStream;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class ObjectStorageDumbBlobStore implements DumbBlobStore {
    private static final int BUFFERED_SIZE = 256 * 1024;

    private final BucketName defaultBucketName;
    private final org.jclouds.blobstore.BlobStore blobStore;
    private final BlobPutter blobPutter;
    private final PayloadCodec payloadCodec;
    private final ObjectStorageBucketNameResolver bucketNameResolver;

    ObjectStorageDumbBlobStore(BucketName defaultBucketName,
                               org.jclouds.blobstore.BlobStore blobStore,
                               BlobPutter blobPutter,
                               PayloadCodec payloadCodec, ObjectStorageBucketNameResolver bucketNameResolver) {
        this.defaultBucketName = defaultBucketName;
        this.blobStore = blobStore;
        this.blobPutter = blobPutter;
        this.payloadCodec = payloadCodec;
        this.bucketNameResolver = bucketNameResolver;
    }



    public static ObjectStorageDumbBlobStoreBuilder builder(SwiftTempAuthObjectStorage.Configuration testConfig) {
        return SwiftTempAuthObjectStorage.blobStoreBuilder(testConfig);
    }

    public static ObjectStorageDumbBlobStoreBuilder builder(SwiftKeystone2ObjectStorage.Configuration testConfig) {
        return SwiftKeystone2ObjectStorage.blobStoreBuilder(testConfig);
    }

    public static ObjectStorageDumbBlobStoreBuilder builder(SwiftKeystone3ObjectStorage.Configuration testConfig) {
        return SwiftKeystone3ObjectStorage.blobStoreBuilder(testConfig);
    }

    public static ObjectStorageDumbBlobStoreBuilder builder(AwsS3AuthConfiguration testConfig) {
        return AwsS3ObjectStorage.blobStoreBuilder(testConfig);
    }

    @PreDestroy
    public void close() {
        blobStore.getContext().close();
    }

    @Override
    public Mono<byte[]> readBytes(BucketName bucketName, BlobId blobId) {
        return Mono.fromCallable(() -> IOUtils.toByteArray(read(bucketName, blobId)));
    }

    @Override
    public Mono<Void> save(BucketName bucketName, BlobId blobId, byte[] data) {
        Preconditions.checkNotNull(data);
        ObjectStorageBucketName resolvedBucketName = bucketNameResolver.resolve(bucketName);

        Payload payload = payloadCodec.write(data);

        Blob blob = blobStore.blobBuilder(blobId.asString())
            .payload(payload.getPayload())
            .contentLength(payload.getLength().orElse(Long.valueOf(data.length)))
            .build();

        return blobPutter.putDirectly(resolvedBucketName, blob);
    }

    @Override
    public Mono<Void> save(BucketName bucketName, BlobId blobId, InputStream inputStream) {
        Preconditions.checkNotNull(inputStream);

        boolean notEager = false;
        return Mono.using(
            () -> new FileBackedOutputStream(1024 * 1024),
            fileBackedOutputStream -> Mono.fromCallable(() -> IOUtils.copy(inputStream, fileBackedOutputStream))
                .map(any -> fileBackedOutputStream.asByteSource())
                .flatMap(byteSource -> {
                    System.out.println("saving");
                    return save(bucketName, blobId, byteSource);
                })
                .doOnNext(any -> System.out.println("end saving"))
                .onErrorMap(IOException.class, e -> new IOObjectStoreException("can't save input stream", e)),
            Throwing.<FileBackedOutputStream>consumer(fileBackedOutputStream -> {
                System.out.println("reseting FileBackedOutputStream");
                fileBackedOutputStream.reset();
            }).sneakyThrow(),
            notEager);
    }

    @Override
    public Mono<Void> save(BucketName bucketName, BlobId blobId, ByteSource content) {
        ObjectStorageBucketName resolvedBucketName = bucketNameResolver.resolve(bucketName);

        return Mono.fromCallable(() -> {
            Payload payload = payloadCodec.write(content);
            return blobStore.blobBuilder(blobId.asString())
                .payload(payload.getPayload())
                .contentLength(payload.getLength().orElse(content.size()))
                .build();
        })
            .flatMap(blob -> blobPutter.putDirectly(resolvedBucketName, blob))
            .onErrorMap(IOException.class, e -> new IOObjectStoreException("Error when saving blob", e));
    }

    @Override
    public InputStream read(BucketName bucketName, BlobId blobId) throws ObjectStoreException {
        ObjectStorageBucketName resolvedBucketName = bucketNameResolver.resolve(bucketName);

        try {
            Blob blob = blobStore.getBlob(resolvedBucketName.asString(), blobId.asString());
            if (blob != null) {
                return payloadCodec.read(new Payload(blob.getPayload(), Optional.empty()));
            } else {
                throw new ObjectNotFoundException("fail to load blob with id " + blobId);
            }
        } catch (HttpResponseException | IOException cause) {
            throw new ObjectStoreException(
                "Failed to readBytes blob " + blobId.asString(),
                cause);
        }
    }

    public BucketName getDefaultBucketName() {
        return defaultBucketName;
    }

    @Override
    public Mono<Void> deleteBucket(BucketName bucketName) {
        ObjectStorageBucketName resolvedBucketName = bucketNameResolver.resolve(bucketName);
        return Mono.<Void>fromRunnable(() -> blobStore.deleteContainer(resolvedBucketName.asString()))
            .subscribeOn(Schedulers.elastic());
    }

    public PayloadCodec getPayloadCodec() {
        return payloadCodec;
    }

    @VisibleForTesting
    Mono<Void> deleteAllBuckets() {
        return Flux.fromIterable(blobStore.list())
            .publishOn(Schedulers.elastic())
            .filter(storageMetadata -> storageMetadata.getType().equals(StorageType.CONTAINER))
            .map(StorageMetadata::getName)
            .doOnNext(blobStore::deleteContainer)
            .then();
    }

    @Override
    public Mono<Void> delete(BucketName bucketName, BlobId blobId) {
        ObjectStorageBucketName resolvedBucketName = bucketNameResolver.resolve(bucketName);
        return Mono.<Void>fromRunnable(() -> blobStore.removeBlob(resolvedBucketName.asString(), blobId.asString()))
            .subscribeOn(Schedulers.elastic());
    }

    @VisibleForTesting BlobStore getBlobStore() {
        return blobStore;
    }
}
