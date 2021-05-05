/*
 * Copyright (2021) The Hyperspace Project Authors.
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

case class Version(major: Int, minor: Int, patch: Int) extends Ordered[Version] {

  def short: String = s"$major.$minor"

  override def toString: String = s"$major.$minor.$patch"

  override def compare(that: Version): Int = {
    import scala.math.Ordered.orderingToOrdered
    (major, minor, patch) compare (that.major, that.minor, that.patch)
  }
}
