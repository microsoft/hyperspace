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

package com.microsoft.hyperspace.shim

import scala.annotation.{compileTimeOnly, StaticAnnotation}
import scala.language.experimental.macros
import scala.reflect.macros.whitebox

import com.microsoft.hyperspace.BuildInfo

@compileTimeOnly("enable macro paradise to expand macro annotations")
class SinceSpark30 extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro SinceSpark30Macro.impl
}

object SinceSpark30Macro {
  def impl(c: whitebox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = annottees match {
    case Seq(method) =>
      import scala.math.Ordering.Implicits._
      import c.universe._
      val Array(major, minor) = BuildInfo.sparkShortVersion.split('.').map(_.toInt)
      if ((major, minor) >= (3, 0)) {
        method
      } else {
        c.Expr[Nothing](EmptyTree)
      }
    case _ =>
      throw new IllegalArgumentException("Please annotate single expressions")
  }
}
