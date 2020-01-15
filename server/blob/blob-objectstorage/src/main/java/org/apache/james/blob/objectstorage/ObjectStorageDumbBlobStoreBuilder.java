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

import java.util.Optional;
import java.util.function.Supplier;

import org.apache.james.blob.api.BucketName;
import org.jclouds.blobstore.BlobStore;

import com.google.common.annotations.VisibleForTesting;

public class ObjectStorageDumbBlobStoreBuilder {

    public static ObjectStorageDumbBlobStoreBuilder forBlobStoreBuilder(Supplier<BlobStore> blobStoreBuilder) {
        return new ObjectStorageDumbBlobStoreBuilder(blobStoreBuilder);
    }


    private final Supplier<BlobStore> supplier;
    private Optional<PayloadCodec> payloadCodec;
    private Optional<BlobPutter> blobPutter;
    private Optional<BucketName> namespace;
    private Optional<String> bucketPrefix;

    private ObjectStorageDumbBlobStoreBuilder(Supplier<BlobStore> supplier) {
        this.payloadCodec = Optional.empty();
        this.supplier = supplier;
        this.blobPutter = Optional.empty();
        this.namespace = Optional.empty();
        this.bucketPrefix = Optional.empty();
    }

    public ObjectStorageDumbBlobStoreBuilder payloadCodec(PayloadCodec payloadCodec) {
        this.payloadCodec = Optional.of(payloadCodec);
        return this;
    }

    public ObjectStorageDumbBlobStoreBuilder payloadCodec(Optional<PayloadCodec> payloadCodec) {
        this.payloadCodec = payloadCodec;
        return this;
    }

    public ObjectStorageDumbBlobStoreBuilder blobPutter(Optional<BlobPutter> blobPutter) {
        this.blobPutter = blobPutter;
        return this;
    }

    public ObjectStorageDumbBlobStoreBuilder namespace(Optional<BucketName> namespace) {
        this.namespace = namespace;
        return this;
    }

    public ObjectStorageDumbBlobStoreBuilder namespace(BucketName namespace) {
        this.namespace = Optional.ofNullable(namespace);
        return this;
    }

    public ObjectStorageDumbBlobStoreBuilder bucketPrefix(Optional<String> bucketPrefix) {
        this.bucketPrefix = bucketPrefix;
        return this;
    }

    public ObjectStorageDumbBlobStoreBuilder bucketPrefix(String prefix) {
        this.bucketPrefix = Optional.ofNullable(prefix);
        return this;
    }

    public ObjectStorageDumbBlobStore build() {
        BlobStore blobStore = supplier.get();

        ObjectStorageBucketNameResolver bucketNameResolver = ObjectStorageBucketNameResolver.builder()
            .prefix(bucketPrefix)
            .namespace(namespace)
            .build();

        return new ObjectStorageDumbBlobStore(namespace.orElse(BucketName.DEFAULT),
            blobStore,
            blobPutter.orElseGet(() -> defaultPutBlob(blobStore)),
            payloadCodec.orElse(PayloadCodec.DEFAULT_CODEC),
            bucketNameResolver);
    }

    private BlobPutter defaultPutBlob(BlobStore blobStore) {
        return new StreamCompatibleBlobPutter(blobStore);
    }

    @VisibleForTesting
    Supplier<BlobStore> getSupplier() {
        return supplier;
    }

}
