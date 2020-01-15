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

import static org.apache.james.blob.api.DumbBlobStoreFixture.TEST_BUCKET_NAME;

import java.util.Optional;

import org.apache.james.blob.api.BucketName;
import org.apache.james.blob.api.DumbBlobStore;
import org.apache.james.blob.api.DumbBlobStoreContract;
import org.apache.james.blob.api.DumbBlobStoreFixture;
import org.apache.james.blob.objectstorage.aws.AwsS3AuthConfiguration;
import org.apache.james.blob.objectstorage.aws.AwsS3ObjectStorage;
import org.apache.james.blob.objectstorage.aws.DockerAwsS3Container;
import org.apache.james.blob.objectstorage.aws.DockerAwsS3Extension;
import org.apache.james.blob.objectstorage.swift.Credentials;
import org.apache.james.blob.objectstorage.swift.Identity;
import org.apache.james.blob.objectstorage.swift.PassHeaderName;
import org.apache.james.blob.objectstorage.swift.SwiftTempAuthObjectStorage;
import org.apache.james.blob.objectstorage.swift.TenantName;
import org.apache.james.blob.objectstorage.swift.UserHeaderName;
import org.apache.james.blob.objectstorage.swift.UserName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(DockerAwsS3Extension.class)
public class ObjectStorageS3DumbBlobStoreTest implements DumbBlobStoreContract {

    private ObjectStorageDumbBlobStore testee;

    @BeforeEach
    void setUp(DockerAwsS3Container dockerAwsS3) {

        BucketName defaultBucketName = TEST_BUCKET_NAME;

        AwsS3ObjectStorage awsS3ObjectStorage = new AwsS3ObjectStorage();

        AwsS3AuthConfiguration configuration = AwsS3AuthConfiguration.builder()
            .endpoint(dockerAwsS3.getEndpoint())
            .accessKeyId(DockerAwsS3Container.ACCESS_KEY_ID)
            .secretKey(DockerAwsS3Container.SECRET_ACCESS_KEY)
            .build();

        testee = ObjectStorageDumbBlobStore
            .builder(configuration)
            .namespace(defaultBucketName)
            .blobPutter(awsS3ObjectStorage.putBlob(configuration))
            .build();
    }

    @AfterEach
    void tearDown() {
        testee.deleteAllBuckets().block();
        testee.close();
    }

    @Override
    public DumbBlobStore testee() {
        return testee;
    }

}
