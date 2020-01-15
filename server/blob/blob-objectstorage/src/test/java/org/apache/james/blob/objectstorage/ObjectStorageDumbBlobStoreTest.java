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

import org.apache.james.blob.api.BucketName;
import org.apache.james.blob.api.DumbBlobStore;
import org.apache.james.blob.api.DumbBlobStoreContract;
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

@ExtendWith(DockerSwiftExtension.class)
public class ObjectStorageDumbBlobStoreTest implements DumbBlobStoreContract {

    private static final TenantName TENANT_NAME = TenantName.of("test");
    private static final UserName USER_NAME = UserName.of("tester");
    private static final Credentials PASSWORD = Credentials.of("testing");
    private static final Identity SWIFT_IDENTITY = Identity.of(TENANT_NAME, USER_NAME);

    private BucketName defaultBucketName;
    private SwiftTempAuthObjectStorage.Configuration testConfig;
    private ObjectStorageDumbBlobStore testee;

    @BeforeEach
    void setUp(DockerSwift dockerSwift) {
        defaultBucketName = BucketName.of("e4fc2427-f2aa-422a-a535-3df0d2a086c4");
        testConfig = SwiftTempAuthObjectStorage.configBuilder()
            .endpoint(dockerSwift.swiftEndpoint())
            .identity(SWIFT_IDENTITY)
            .credentials(PASSWORD)
            .tempAuthHeaderUserName(UserHeaderName.of("X-Storage-User"))
            .tempAuthHeaderPassName(PassHeaderName.of("X-Storage-Pass"))
            .build();
        testee = ObjectStorageDumbBlobStore
            .builder(testConfig)
            .namespace(defaultBucketName)
            .build();

        //testee.getBlobStore().createContainerInLocation(null, TEST_BUCKET_NAME.asString());
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
