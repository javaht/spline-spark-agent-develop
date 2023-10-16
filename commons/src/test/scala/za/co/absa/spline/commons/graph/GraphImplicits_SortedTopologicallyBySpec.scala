/*
 * Copyright 2023 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.spline.commons.graph

import za.co.absa.spline.commons.graph.GraphImplicits._

class GraphImplicits_SortedTopologicallyBySpec
  extends AbstractGraphImplicits_SortedTopologicallySpec(
    "sortedTopologicallyBy",
    // `toSeq` is required for Scala 2.13
    // noinspection RedundantCollectionConversion
    _.toSeq.sortedTopologicallyBy(_._1, _._2)
  )
