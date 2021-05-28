/*
 * Copyright (2020) The Hyperspace Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.hyperspace.util.fingerprint

import org.apache.commons.codec.digest.DigestUtils

/**
 * A factory trait for FingerprintBuilder. This trait must be implemented without any state.
 * The same instance of this trait can be shared and used by many objects.
 */
trait FingerprintBuilderFactory {

  /**
   * Creates a fingerprint builder.
   */
  def create: FingerprintBuilder
}

/**
 * MD5-based fingerprint builder. The fingerprint has 16 bytes (128 bits).
 */
class MD5FingerprintBuilderFactory extends FingerprintBuilderFactory {
  override def create: FingerprintBuilder = {
    new FingerprintBuilder(DigestUtils.getMd5Digest)
  }
}

/**
 * SHA1-based fingerprint builder. The fingerprint has 20 bytes (160 bits).
 */
class SHA1FingerprintBuilderFactory extends FingerprintBuilderFactory {
  override def create: FingerprintBuilder = {
    new FingerprintBuilder(DigestUtils.getSha1Digest)
  }
}
