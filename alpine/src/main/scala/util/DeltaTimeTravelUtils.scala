/*
 * Copyright (2020) The Delta Lake Project Authors.
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


package main.scala.util

import java.sql.Timestamp
import java.text.ParseException
import java.time.ZoneId
import java.util.TimeZone

import org.apache.commons.lang3.time.FastDateFormat

object DeltaTimeTravelUtils {
  final val TIMESTAMP_FORMAT = "yyyyMMddHHmmssSSS"

  /**
   * @throws `ParseException` when the timestamp format doesn't match our criteria
   * @param timestamp
   * @param timeZone
   * @return
   */
  def parseTimestamp(timestamp: String, timeZone: String = "UTC"): Timestamp = {
    val dateFormat = FastDateFormat.getInstance(TIMESTAMP_FORMAT, getTimeZone(timeZone))
    new Timestamp(dateFormat.parse(timestamp).getTime)
  }

  private def getZoneId(timeZoneId: String) = ZoneId.of(timeZoneId, ZoneId.SHORT_IDS)

  private def getTimeZone(timeZoneId: String) = TimeZone.getTimeZone(getZoneId(timeZoneId))
}
